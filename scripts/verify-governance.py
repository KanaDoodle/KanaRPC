#!/usr/bin/env python3
"""Serial open-loop fixture controls; all offers and negative outcomes kept."""
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
PROFILES = ["fixed-fifo", "fixed-fair", "fixed4-fair", "fixed8-fair", "adaptive", "feedback", "guarded", "guarded-feedback"]
SCENES = ["low-load", "steady", "capacity-shift", "unattainable"]


def cpu():
    value = resource.getrusage(resource.RUSAGE_CHILDREN)
    return value.ru_utime + value.ru_stime


def wait_json(path, process):
    until = time.monotonic() + 10
    while time.monotonic() < until:
        if path.exists():
            return json.loads(path.read_text())
        if process.poll() is not None:
            raise RuntimeError("owned worker exited before ready")
        time.sleep(.01)
    raise TimeoutError("worker startup deadline")


def run(binary, directory, profile, scene, transport, seed, duration):
    directory.mkdir()
    record = {"profile": profile, "scene": scene, "transport": transport, "seed": seed}
    processes = []
    env = dict(os.environ, GOWORK="off", GOMAXPROCS="2")
    common = ["-profile", profile, "-scene", scene, "-transport", transport, "-ready", str(directory / "ready.json"), "-duration", f"{duration}s"]
    before = cpu()
    with (directory / "worker.log").open("wb") as server_log, (directory / "client.log").open("wb") as client_log:
        try:
            worker = subprocess.Popen([str(binary), "-mode", "worker", "-output", str(directory / "worker.json"), *common], stdout=server_log, stderr=server_log, env=env)
            processes.append(worker)
            ready = wait_json(directory / "ready.json", worker)
            client = subprocess.Popen([str(binary), "-mode", "client", "-output", str(directory / "client.json"), "-seed", str(seed), *common], stdout=client_log, stderr=client_log, env=env)
            processes.append(client)
            if client.wait(timeout=duration + 20):
                raise RuntimeError("client process failed")
            client_result = json.loads((directory / "client.json").read_text())
            record["client"] = {k: v for k, v in client_result.items() if k not in {"offers", "samples"}}
            assert sum(sum(group.values()) for group in client_result["counts"].values()) == client_result["offered"]
            unexpected = [row for row in client_result["offers"] if row["Result"].startswith("transport_error") or row["Result"] == "corrupt_response"]
            assert not unexpected, unexpected[:3]
            until = time.monotonic() + 5
            while time.monotonic() < until:
                with urllib.request.urlopen(ready["Admin"] + "/stats", timeout=1) as response:
                    live = json.load(response)
                if all(live["stats"][key] == 0 for key in ["InFlight", "Executing", "Queued", "NativeStreams"]):
                    break
                time.sleep(.01)
            else:
                raise RuntimeError("server retained work after offers settled")
            worker.terminate()
            if worker.wait(timeout=8):
                raise RuntimeError("worker drain failed")
            record["worker"] = json.loads((directory / "worker.json").read_text())
            assert record["worker"]["duplicate_ids"] == 0
            assert all(record["worker"]["stats"][key] == 0 for key in ["InFlight", "Executing", "Queued", "NativeStreams", "Connections", "LostTerminals", "IncompleteExchanges"])
            for memory in [record["worker"]["memory"], *[c["memory"] for c in client_result["clients"]]]:
                assert all(group["Used"] == 0 and group["Peak"] <= group["Limit"] for group in memory.values())
            record["on_time"] = {path: sum(row["Method"] == path and row["Result"] == "ok" and row["Latency"] + row["Lateness"] <= deadline for row in client_result["offers"]) for path, deadline in [("/lab/fast", 20_000_000), ("/lab/slow", 50_000_000)]}
            record["status"] = "PASS"
        except BaseException as error:
            record["status"] = "FAIL"
            record["failure"] = repr(error)
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
            record["pair_cpu_seconds"] = cpu() - before
            record["owned_processes_reaped"] = all(p.poll() is not None for p in processes)
            (directory / "result.json").write_text(json.dumps(record, ensure_ascii=False, indent=2) + "\n")
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--duration", type=float, default=2)
    parser.add_argument("--scenes", choices=SCENES, nargs="+", default=SCENES)
    parser.add_argument("--profiles", choices=PROFILES, nargs="+", default=PROFILES)
    parser.add_argument("--seeds", nargs="+", type=int, default=[7, 19, 41])
    parser.add_argument("--cross-transport", action="store_true")
    parser.add_argument("--cross-profile", choices=PROFILES, default="guarded-feedback")
    parser.add_argument("--race", action="store_true", help="race-instrument the actual worker/client; not performance evidence")
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary = output / "governance-lab"
    subprocess.run(["go", "build", *(["-race"] if args.race else []), "-o", str(binary), "./cmd/governance-lab"], cwd=ROOT, env=dict(os.environ, GOWORK="off"), check=True)
    metadata = {"go_version": subprocess.check_output(["go", "version"], text=True).strip(), "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(), "duration": args.duration, "seeds": args.seeds, "profiles": args.profiles, "scenes": args.scenes, "source_sha256": {str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(ROOT.rglob("*.go")) if "tools" not in p.relative_to(ROOT).parts}, "scope": "real serial loopback open-loop RPC, 4 clients, generator max 128 and lateness gate 50ms; dynamic-slot/timer fixture, no model or production-capacity claim"}
    for name in ["go.mod", "go.sum"]:
        metadata["source_sha256"][name] = hashlib.sha256((ROOT/name).read_bytes()).hexdigest()
    metadata["race_instrumented"] = args.race
    metadata["driver_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    metadata["gomaxprocs"] = 2
    metadata["cross_profile"] = args.cross_profile
    if args.race:
        metadata["scope"] += "; race instrumentation: correctness only, exclude timings from performance conclusions"
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    cases = []
    try:
        for repeat, seed in enumerate(args.seeds):
            order = args.profiles[repeat % len(args.profiles):] + args.profiles[:repeat % len(args.profiles)]
            for scene in args.scenes:
                for profile in order:
                    name = f"tcp-{scene}-{profile}-{seed}"
                    cases.append(run(binary, output/name, profile, scene, "tcp", seed, args.duration))
        if args.cross_transport:
            for seed in args.seeds:
                for transport in ["tls", "quic"]:
                    name = f"{transport}-capacity-shift-{args.cross_profile}-{seed}"
                    cases.append(run(binary, output/name, args.cross_profile, "capacity-shift", transport, seed, args.duration))
    finally:
        records = [json.loads((path/"result.json").read_text()) for path in sorted(output.iterdir()) if path.is_dir() and (path/"result.json").exists()]
        (output / "summary.json").write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n")
    lines = ["# RPC open-loop fixture controls", "", metadata["scope"], "", "| Scene | Profile | Offered | OK | Fast OK/s | Fast p95 ms | On-time total | Pair CPU s |", "|---|---|---:|---:|---:|---:|---:|---:|"]
    for scene in args.scenes:
        for profile in args.profiles:
            group = [row for row in cases if row["scene"] == scene and row["profile"] == profile and row["transport"] == "tcp"]
            average = lambda function: statistics.mean(function(row) for row in group)
            lines.append(f"| {scene} | {profile} | {sum(row['client']['offered'] for row in group)} | {sum(sum(v.get('ok',0) for v in row['client']['counts'].values()) for row in group)} | {average(lambda row: row['client']['counts']['/lab/fast'].get('ok',0)/(row['client']['elapsed_ns']/1e9)):.1f} | {average(lambda row: row['client']['latency_success_only']['/lab/fast']['p95']/1e6):.3f} | {sum(sum(row['on_time'].values()) for row in group)} | {average(lambda row: row['pair_cpu_seconds']):.3f} |")
    (output / "summary.md").write_text("\n".join(lines) + "\n")
    print(json.dumps({"status": "PASS", "cases": len(cases), "offered": sum(row["client"]["offered"] for row in cases)}))


if __name__ == "__main__":
    main()
