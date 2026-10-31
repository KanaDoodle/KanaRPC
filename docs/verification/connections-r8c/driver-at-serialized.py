#!/usr/bin/env python3
"""Serial two-worker/one-client controls with complete offered results."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def cpu():
    value = resource.getrusage(resource.RUSAGE_CHILDREN)
    return value.ru_utime + value.ru_stime


def case(worker_binary, client_binary, directory, transport, workload, strategy, seed, count, workers):
    directory.mkdir()
    env = dict(os.environ, GOWORK="off", GOMAXPROCS="2")
    processes, logs, ready = [], [], []
    result = dict(transport=transport, workload=workload, strategy=strategy, seed=seed)
    before = cpu()
    try:
        for index in range(2):
            log = (directory/f"worker-{index}.log").open("wb"); logs.append(log)
            path = directory/f"ready-{index}.json"
            worker = subprocess.Popen([str(worker_binary), "-mode", "worker", "-transport", transport, "-ready", str(path), "-output", str(directory/f"worker-{index}.json")], stdout=log, stderr=log, env=env)
            processes.append(worker)
            until = time.monotonic()+15
            while not path.exists():
                if worker.poll() is not None or time.monotonic()>until:
                    raise RuntimeError("owned worker startup failed")
                time.sleep(.01)
            ready.append(json.loads(path.read_text()))
        log = (directory/"client.log").open("wb"); logs.append(log)
        client = subprocess.Popen([str(client_binary), "-ready", ",".join(str(directory/f"ready-{i}.json") for i in range(2)), "-output", str(directory/"client.json"), "-strategy", strategy, "-workload", workload, "-seed", str(seed), "-count", str(count), "-workers", str(workers)], stdout=log, stderr=log, env=env)
        processes.append(client)
        code = client.wait(timeout=120)
        if (directory/"client.json").exists():
            result["client"] = json.loads((directory/"client.json").read_text())
        if code not in [0,1] or 'client' not in result:
            raise RuntimeError(f"client exited {code}; outcomes retained")
        client_result=result['client']
        # Completed runs with explicit failed offers are negative measurements,
        # not reasons to discard the rest of the matrix. Crashes, warmup/data
        # corruption, incomplete cleanup and duplicate executions still fail.
        if client_result['wait_error'] or any('corrupt' in row.get('error','') or row.get('error','').startswith('terminal:') for row in client_result['offers']):
            raise RuntimeError('integrity or cleanup failure')
        for index in range(2):
            until = time.monotonic()+5
            while True:
                with urllib.request.urlopen(ready[index]["admin"]+"/stats", timeout=1) as response:
                    live = json.load(response)
                if all(live[key]==0 for key in ["InFlight", "Executing", "NativeStreams"]):
                    break
                if time.monotonic()>until:
                    raise RuntimeError("server exchanges not retired")
                time.sleep(.01)
            processes[index].terminate()
            if processes[index].wait(timeout=8):
                raise RuntimeError("owned worker drain failed")
        result["workers"] = [json.loads((directory/f"worker-{i}.json").read_text()) for i in range(2)]
        assert client_result['completed']+64 <= sum(w["executions"] for w in result["workers"]) <= count+64
        for worker in result["workers"]:
            assert worker["duplicates"]==0
            assert all(worker["stats"][key]==0 for key in ["Connections", "InFlight", "Executing", "NativeStreams", "LostTerminals", "IncompleteExchanges"])
        assert code==1 and client_result['completed']<count or code==0 and client_result['completed']==count
        managed = result["client"]["stats"]
        if managed:
            assert managed["Closed"] and managed["Connections"]==managed["ActiveCalls"]==0
            assert managed["PeakConnections"] <= (1 if strategy=="churn" else 2)
        result["status"] = "DEGRADED" if code else "PASS"
    except BaseException as error:
        result.update(status="FAIL", failure=repr(error))
        raise
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:process.wait(timeout=5)
                except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
        for log in logs:log.close()
        result["process_triple_cpu_seconds"] = cpu()-before
        result["owned_processes_reaped"] = all(p.poll() is not None for p in processes)
        (directory/"result.json").write_text(json.dumps(result, indent=2)+"\n")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--seeds", nargs="+", type=int, default=[7,19,41])
    parser.add_argument("--transports", nargs="+", choices=["tcp","tls","quic"], default=["tcp","quic"])
    parser.add_argument("--strategies", nargs="+", choices=["direct","dial","managed","churn"], default=["direct","dial","managed","churn"])
    parser.add_argument("--race", action="store_true")
    args = parser.parse_args(); output=args.output.resolve();output.mkdir(parents=True, exist_ok=False)
    binaries = {}
    for name in ["transport-lab","routing-lab"]:
        binary=output/name
        subprocess.run(["go","build",*(["-race"] if args.race else []),"-o",str(binary),"./cmd/"+name],cwd=ROOT,env=dict(os.environ,GOWORK="off"),check=True)
        binaries[name]=binary
    metadata=dict(go_version=subprocess.check_output(["go","version"],text=True).strip(),count=args.count,workers=args.workers,seeds=args.seeds,race=args.race,GOMAXPROCS=2,scope="serial closed-loop loopback, two real workers, fixed local discovery snapshot fixture; direct reuses two connections, dial creates one per call, managed cap=2, churn cap=1; 64 warmups excluded from latency but included in whole-process CPU; not real inference/discovery propagation/cross-machine performance")
    metadata["source_sha256"]={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(ROOT.rglob("*.go")) if not any(v in p.relative_to(ROOT).parts for v in ["docs","tools"])}
    for name in ["go.mod","go.sum","scripts/verify-routing.py"]:metadata["source_sha256"][name]=hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
    metadata["binary_sha256"]={name:hashlib.sha256(path.read_bytes()).hexdigest() for name,path in binaries.items()}
    (output/"metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
    try:
        for repeat,seed in enumerate(args.seeds):
            for workload in ["unary","stream"]:
                for transport in args.transports:
                    offset=repeat%len(args.strategies);order=args.strategies[offset:]+args.strategies[:offset]
                    for strategy in order:
                        result=case(binaries["transport-lab"],binaries["routing-lab"],output/f"{seed}-{workload}-{transport}-{strategy}",transport,workload,strategy,seed,args.count,args.workers)
                        print(seed,workload,transport,strategy,result['status'],flush=True)
    finally:
        rows=[]
        for path in sorted(output.glob("*/result.json")):
            row=json.loads(path.read_text())
            if "client" in row:row["client"]={k:v for k,v in row["client"].items() if k!="offers"}
            rows.append(row)
        (output/"summary.json").write_text(json.dumps(rows,indent=2)+"\n")
    print(json.dumps(dict(status="PASS_WITH_DEGRADATION" if any(row['status']!='PASS' for row in rows) else "PASS",cases=len(rows),offered=len(rows)*args.count,degraded_cases=sum(row['status']=='DEGRADED' for row in rows))))


if __name__=="__main__":main()
