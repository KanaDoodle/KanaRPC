#!/usr/bin/env python3
"""Serialized, owned R8A/R8B resource and transport controls; no model."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]


def build(root, output, race=False):
    binary = output / "resource-lab"
    subprocess.run(["go", "build", *(["-race"] if race else []), "-o", str(binary), "./cmd/resource-lab"], cwd=root, env=dict(os.environ, GOWORK="off"), check=True)
    return binary


def soak(binary, output, transport, duration, cycles):
    output.mkdir()
    processes = []
    result = {"transport": transport}
    env = dict(os.environ, GOWORK="off", GOMAXPROCS="2")
    with (output/"worker.log").open("wb") as worker_log, (output/"client.log").open("wb") as client_log:
        try:
            common = ["-ready",str(output/"ready.json"),"-transport",transport]
            worker = subprocess.Popen([str(binary),"-mode","worker","-output",str(output/"worker.json"),*common],stdout=worker_log,stderr=worker_log,env=env)
            processes.append(worker)
            until = time.monotonic()+10
            while not (output/"ready.json").exists():
                if worker.poll() is not None or time.monotonic()>until:raise RuntimeError("worker startup failed")
                time.sleep(.01)
            client = subprocess.Popen([str(binary),"-mode","client","-output",str(output/"client.json"),"-duration",str(duration)+"s","-cycles",str(cycles),*common],stdout=client_log,stderr=client_log,env=env)
            processes.append(client)
            if client.wait(timeout=duration+45):raise RuntimeError("resource client failed")
            result["client"] = json.loads((output/"client.json").read_text())
            # Parse after the measured process exits: observation history must
            # not inflate that process's heap. Keep compatibility with old labs.
            for key in ['phases', 'samples']:
                path=output/("client.json."+key+".jsonl")
                if path.exists():result['client'][key]=[json.loads(line) for line in path.read_text().splitlines()]
            worker.terminate()
            if worker.wait(timeout=8):raise RuntimeError("worker drain failed")
            result["worker"] = json.loads((output/"worker.json").read_text())
            stats = result["worker"]["rpc"]
            assert all(stats[key]==0 for key in ["Connections","InFlight","Executing","NativeStreams","LostTerminals","IncompleteExchanges"]),stats
            for phase in result["client"]["phases"]:
                for mem in [phase["client"]["MemoryStats"],phase["server"]["memory"]]:
                    assert all(v["Used"]==0 and v["Peak"]<=v["Limit"] for v in mem.values()),mem
                assert phase["completed_unread"]["MemoryStats"]["Receive"]["Used"]>0
            final = result["client"]["final_client"]
            assert all(v["Used"]==0 and v["Peak"]<=v["Limit"] for v in final["MemoryStats"].values())
            if "AdmissionStats" in final:
                assert all(final["AdmissionStats"][key]==0 for key in ["Active","Preparing","Encoding","EncodingWaiters"])
                assert final["AdmissionStats"]["Peak"]<=final["AdmissionStats"]["Limit"]
            result["status"]="PASS"
        except BaseException as error:
            result.update(status="FAIL",failure=repr(error));raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:process.wait(timeout=5)
                    except subprocess.TimeoutExpired:process.kill();process.wait(timeout=3)
            result["owned_processes_reaped"]=all(p.poll() is not None for p in processes)
            (output/"result.json").write_text(json.dumps(result,indent=2)+"\n")
    return result


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",required=True,type=Path)
    parser.add_argument("--duration",type=int,default=60)
    parser.add_argument("--cycles",type=int,default=6)
    parser.add_argument("--transports",nargs="+",choices=["tcp","tls","quic"],default=["tcp","quic"])
    parser.add_argument("--baseline",default="d595a2d")
    parser.add_argument("--race",action="store_true")
    parser.add_argument("--no-performance",action="store_true")
    args=parser.parse_args()
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    baseline=output/"baseline-source";baseline.mkdir()
    source=subprocess.run(["git","archive",args.baseline],cwd=ROOT,capture_output=True,check=True).stdout
    import io,tarfile
    with tarfile.open(fileobj=io.BytesIO(source)) as archive:archive.extractall(baseline,filter="data")
    (baseline/"cmd/resource-lab").mkdir(exist_ok=True)
    shutil.copy2(ROOT/"cmd/resource-lab/main.go",baseline/"cmd/resource-lab/main.go")
    # Freeze current code before any long run. Later milestones may proceed in
    # the workspace without changing the code used by this phase's binaries.
    current=output/"current-source";current.mkdir()
    for path in ROOT.rglob('*.go'):
        rel=path.relative_to(ROOT)
        if any(x in rel.parts for x in ['tools','docs','.git']):continue
        target=current/rel;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(path,target)
    for name in ['go.mod','go.sum']:shutil.copy2(ROOT/name,current/name)
    (current/'scripts').mkdir();shutil.copy2(ROOT/'scripts/verify-transports.py',current/'scripts/verify-transports.py')
    binaries={}
    metadata={"go_version":subprocess.check_output(["go","version"],text=True).strip(),"baseline_commit":subprocess.check_output(["git","rev-parse",args.baseline],cwd=ROOT,text=True).strip(),"duration":args.duration,"cycles":args.cycles,"race":args.race,"scope":"bounded loopback resource observation; same added resource driver on R8A baseline; no real inference or infinite-leak/RSS claim","versions":{}}
    for name,root in [("baseline",baseline),("current",current)]:
        directory=output/name;directory.mkdir();binaries[name]=build(root,directory,args.race)
        metadata["versions"][name]={"binary_sha256":hashlib.sha256(binaries[name].read_bytes()).hexdigest(),"source_sha256":{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(root.rglob('*.go')) if not any(x in p.relative_to(root).parts for x in ['tools','docs'])}}
        for file in ['go.mod','go.sum']:metadata['versions'][name]['source_sha256'][file]=hashlib.sha256((root/file).read_bytes()).hexdigest()
    metadata['driver_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    (output/"metadata.json").write_text(json.dumps(metadata,indent=2)+"\n")
    results=[]
    try:
        for index,transport in enumerate(args.transports):
            order=["baseline","current"] if index%2==0 else ["current","baseline"]
            for version in order:
                result=soak(binaries[version],output/(version+"-"+transport),transport,args.duration,args.cycles);result["version"]=version;results.append(result)
                print(version,transport,"PASS",flush=True)
        if not args.no_performance:
            spec=importlib.util.spec_from_file_location("transport_controls",current/"scripts/verify-transports.py");controls=importlib.util.module_from_spec(spec);spec.loader.exec_module(controls)
            perf={}
            for name,root in [("baseline",baseline),("current",current)]:
                binary=output/name/"transport-lab";subprocess.run(["go","build","-o",str(binary),"./cmd/transport-lab"],cwd=root,env=dict(os.environ,GOWORK="off"),check=True);perf[name]=binary
            cases=[]
            for repeat,seed in enumerate([7,19,41]):
                for workload in ["unary","stream"]:
                    for transport in args.transports:
                        for version in (["baseline","current"] if repeat%2==0 else ["current","baseline"]):
                            case=controls.case(perf[version],output/f"perf-{version}-{transport}-{workload}-{seed}",transport,workload,seed,3000,8);case["version"]=version;cases.append(case)
            (output/"performance.json").write_text(json.dumps(cases,indent=2)+"\n")
    finally:
        (output/"summary.json").write_text(json.dumps(results,indent=2)+"\n")
    print(json.dumps({"status":"PASS","soaks":len(results),"active_seconds":len(results)*args.duration}))


if __name__=="__main__":main()
