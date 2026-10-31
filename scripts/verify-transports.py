#!/usr/bin/env python3
"""Serial, owned two-process TCP/TLS/QUIC controls; retain every offered result."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import statistics
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def usage():
    value = resource.getrusage(resource.RUSAGE_CHILDREN)
    return value.ru_utime + value.ru_stime


def load(path, process, timeout=15):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        if path.is_file():
            return json.loads(path.read_text())
        if process.poll() is not None:
            raise RuntimeError(f"process exited before writing {path.name}")
        time.sleep(.01)
    raise TimeoutError(f"process did not write {path.name}")


def case(binary, directory, transport, workload, seed, count, workers):
    directory.mkdir()
    env = dict(os.environ, GOWORK="off", GOMAXPROCS="2")
    before = usage()
    processes = []
    result = {"transport": transport, "workload": workload, "seed": seed}
    with (directory / "worker.log").open("wb") as worker_log, (directory / "client.log").open("wb") as client_log:
        try:
            worker = subprocess.Popen([str(binary), "-mode", "worker", "-transport", transport, "-ready", str(directory / "ready.json"), "-output", str(directory / "worker.json")], stdout=worker_log, stderr=worker_log, env=env)
            processes.append(worker)
            ready = load(directory / "ready.json", worker)
            client = subprocess.Popen([str(binary), "-mode", "client", "-ready", str(directory / "ready.json"), "-output", str(directory / "client.json"), "-workload", workload, "-seed", str(seed), "-count", str(count), "-workers", str(workers)], stdout=client_log, stderr=client_log, env=env)
            processes.append(client)
            code = client.wait(timeout=90)
            if (directory / "client.json").exists():
                result["client"] = json.loads((directory / "client.json").read_text())
            if code:
                raise RuntimeError(f"client exited {code}; failures retained")
            until = time.monotonic() + 5
            while time.monotonic() < until:
                with urllib.request.urlopen(ready["admin"] + "/stats", timeout=1) as response:
                    live = json.load(response)
                if all(live[key] == 0 for key in ["InFlight", "Executing", "NativeStreams"]):
                    break
                time.sleep(.01)
            else:
                raise RuntimeError("worker did not retire requests")
            worker.terminate()
            if worker.wait(timeout=8):
                raise RuntimeError("worker drain failed")
            result["worker"] = json.loads((directory / "worker.json").read_text())
            stats = result["worker"]["stats"]
            assert all(stats[key] == 0 for key in ["Connections", "InFlight", "Executing", "NativeStreams", "LostTerminals", "IncompleteExchanges"]), stats
            assert result["worker"]["duplicates"] == 0
            assert result["worker"]["executions"] == count + 64
            assert result["client"]["completed"] == count
            if transport == "quic":
                native = result["client"]["stats"]
                assert native["Active"] == 0 and native["Opened"] == native["Retired"] == count + 64
                assert native["CloseConfirmed"] is True
            result["status"] = "PASS"
        except BaseException as error:
            result["status"] = "FAIL"
            result["failure"] = repr(error)
            raise
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=3)
            result["process_pair_cpu_seconds"] = usage() - before
            result["owned_processes_reaped"] = all(process.poll() is not None for process in processes)
            (directory / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--count", type=int, default=3000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--seeds", type=int, nargs="+", default=[7, 19, 41])
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary = output / "transport-lab"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/transport-lab"], cwd=ROOT, env=dict(os.environ, GOWORK="off"), check=True)
    metadata = {"go_version": subprocess.check_output(["go", "version"], text=True).strip(), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "count": args.count, "workers": args.workers, "seeds": args.seeds, "GOMAXPROCS": 2, "scope": "serial loopback closed-loop; 64 warmups excluded from request latency, included in whole-process CPU; no model, cross-machine or packet-loss claim"}
    metadata["source_sha256"] = {str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest() for path in sorted(ROOT.rglob("*.go")) if "tools" not in path.relative_to(ROOT).parts}
    for name in ["go.mod", "go.sum"]:
        metadata["source_sha256"][name] = hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    records = []
    try:
        for seed in args.seeds:
            for workload in ["unary", "stream"]:
                # Rotate order to reduce a fixed transport-order bias.
                order = ["tcp", "tls", "quic"]
                offset = args.seeds.index(seed) % 3
                order = order[offset:] + order[:offset]
                for transport in order:
                    records.append(case(binary, output / f"{seed}-{workload}-{transport}", transport, workload, seed, args.count, args.workers))
    finally:
        compact = []
        for directory in sorted(output.iterdir()):
            if directory.is_dir() and (directory / "result.json").exists():
                row = json.loads((directory / "result.json").read_text())
                if "client" in row:
                    row["client"] = {key: value for key, value in row["client"].items() if key != "offers"}
                compact.append(row)
        (output / "summary.json").write_text(json.dumps(compact, ensure_ascii=False, indent=2) + "\n")
    lines = ["# R7B serial transport controls", "", metadata["scope"], "", "| Workload | Transport | Completed/offered | Mean req/s | Mean p95 ms | Mean pair CPU s |", "|---|---|---:|---:|---:|---:|"]
    for workload in ["unary", "stream"]:
        for transport in ["tcp", "tls", "quic"]:
            group = [row for row in records if row["workload"] == workload and row["transport"] == transport]
            lines.append(f"| {workload} | {transport} | {sum(row['client']['completed'] for row in group)}/{len(group)*args.count} | {statistics.mean(row['client']['completed']/(row['client']['elapsed_ns']/1e9) for row in group):.1f} | {statistics.mean(row['client']['P95']/1e6 for row in group):.3f} | {statistics.mean(row['process_pair_cpu_seconds'] for row in group):.4f} |")
    (output / "summary.md").write_text("\n".join(lines) + "\n")
    print(json.dumps({"status": "PASS", "cases": len(records), "offered": len(records) * args.count, "output": str(output)}))


if __name__ == "__main__":
    main()
