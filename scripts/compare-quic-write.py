#!/usr/bin/env python3
"""Serialized historical/current controls with one common payload-aware driver."""
import argparse
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import statistics
import subprocess
import tarfile
import tempfile
import time
import traceback

ROOT = Path(__file__).resolve().parents[1]


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def snapshot(root, ref=None):
    root.mkdir()
    if ref:
        raw = subprocess.check_output(["git", "archive", ref], cwd=ROOT)
        with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
            for member in archive.getmembers():
                path = Path(member.name)
                if member.isfile() and not any(x in path.parts for x in ["docs", "tools"]):
                    if path.suffix == ".go" or member.name in ["go.mod", "go.sum", "LICENSE"]:
                        archive.extract(member, root, filter="data")
    else:
        for path in ROOT.rglob("*.go"):
            relative = path.relative_to(ROOT)
            if any(x in relative.parts for x in ["docs", "tools", ".git"]):
                continue
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
        for name in ["go.mod", "go.sum", "LICENSE"]:
            shutil.copy2(ROOT / name, root / name)
    # An experiment driver is deliberately common to all runtime versions.
    shutil.copy2(ROOT / "cmd/routing-lab/main.go", root / "cmd/routing-lab/main.go")
    shutil.copy2(ROOT / "cmd/transport-lab/main.go", root / "cmd/transport-lab/main.go")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--profile", action="store_true", help="diagnostic CPU/mutex/block/alloc profiles; not throughput evidence")
    parser.add_argument("--seeds", type=int, nargs="+", default=[7, 19, 41, 67, 97])
    parser.add_argument("--payloads", type=int, nargs="+", default=[128, 900, 1024, 4096, 16384])
    parser.add_argument("--transports", nargs="+", choices=["tcp", "tls", "quic"], default=["tcp", "quic"])
    parser.add_argument("--workloads", nargs="+", choices=["unary", "stream"], default=["unary", "stream"])
    parser.add_argument("--strategies", nargs="+", choices=["direct", "managed"], default=["direct", "managed"])
    parser.add_argument("--versions", nargs="+", choices=["r8d", "r9a", "r9b", "r9c", "current"], default=["r8d", "r9a", "current"])
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    work = Path(tempfile.mkdtemp(prefix="kanarpc-r9b-perf-"))
    env = dict(os.environ, GOWORK="off", GOMAXPROCS="2")
    refs = {"r8d": "90af591", "r9a": "478ab3d", "r9b": "0b33dad", "r9c": "c02d770", "current": None}
    metadata = {
        "go": subprocess.check_output(["go", "version"], text=True).strip(),
        "work": str(work), "count": args.count, "workers": args.workers,
        "seeds": args.seeds, "payloads": args.payloads, "GOMAXPROCS": 2,
        "transports": args.transports, "workloads": args.workloads,
        "strategies": args.strategies, "versions": {},
        "profiled_diagnostic_only": args.profile,
        "load_average_at_start": os.getloadavg(), "driver_sha256": digest(Path(__file__)),
        "scope": "serial owned loopback, paired runtime worker/client versions, one common payload-aware driver; direct reuses two connections; managed has two connections and default pool budgets; fixed snapshot fixture; 64 warmups excluded from timed offers; shared-host background load uncontrolled; no real dual-host, inference or production-capacity claim",
    }
    binaries = {}
    roots = {}
    for version in args.versions:
        root = work / version
        snapshot(root, refs[version])
        roots[version] = root
        binaries[version] = {}
        for name in ["routing-lab", "transport-lab"]:
            binary = work / (version + "-" + name)
            subprocess.run(["go", "build", "-o", str(binary), "./cmd/" + name], cwd=root, env=env, check=True)
            binaries[version][name] = binary
        sources = {str(p.relative_to(root)): digest(p) for p in sorted(root.rglob("*.go"))}
        for name in ["go.mod", "go.sum", "LICENSE"]:
            sources[name] = digest(root / name)
        metadata["versions"][version] = {
            "ref": subprocess.check_output(["git", "rev-parse", refs[version]], cwd=ROOT, text=True).strip() if refs[version] else "working snapshot based on " + subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
            "source_sha256": sources,
            "binary_sha256": {name: digest(path) for name, path in binaries[version].items()},
        }
        with tarfile.open(output / (version + "-source.tar.gz"), "w:gz") as archive:
            for relative in sources:
                archive.add(root / relative, arcname=relative)
    if "r9a" in roots and "current" in roots:
        # R9C adds local budget provenance in resources.go; the ledger and
        # routing runtime remain byte-identical to the R9A resource mechanism.
        for path in ["memory_pool.go", "routing/client.go"]:
            assert digest(roots["r9a"] / path) == digest(roots["current"] / path), path
        metadata["pool_ledger_and_routing_identical_to_r9a"] = True
    if "r9b" in roots and "current" in roots:
        for path in ["quic.go", "memory_pool.go", "routing/client.go"]:
            assert digest(roots["r9b"] / path) == digest(roots["current"] / path), path
        metadata["native_write_and_pool_identical_to_r9b"] = True
    controls_path = work / "verify-routing.py"
    shutil.copy2(ROOT / "scripts/verify-routing.py", controls_path)
    metadata["controls_sha256"] = digest(controls_path)
    (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    spec = importlib.util.spec_from_file_location("routing_controls", controls_path)
    controls = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(controls)
    rows = []
    planned = len(args.seeds) * len(args.payloads) * len(args.transports) * len(args.workloads) * len(args.strategies) * len(args.versions)
    try:
        for repeat, seed in enumerate(args.seeds):
            for payload in args.payloads:
                for workload in args.workloads:
                    for transport in args.transports:
                        for strategy in args.strategies:
                            offset = repeat % len(args.versions)
                            order = args.versions[offset:] + args.versions[:offset]
                            for version in order:
                                directory = output / f"{seed}-{payload}-{workload}-{transport}-{strategy}-{version}"
                                try:
                                    result = controls.case(binaries[version]["transport-lab"], binaries[version]["routing-lab"], directory, transport, workload, strategy, seed, args.count, args.workers, payload, profile=args.profile)
                                    memory = result["client"].get("memory")
                                    if strategy == "managed" and version != "r8d":
                                        assert memory is not None
                                        assert memory["Accounts"] == memory["Total"]["Used"] == 0
                                        assert memory["Created"] == memory["Released"]
                                        assert memory["Data"]["Peak"] <= memory["Data"]["Limit"]
                                        assert memory["Total"]["Peak"] <= memory["Total"]["Limit"]
                                        result["pool_invariants"] = "PASS"
                                except Exception as error:
                                    path = directory / "result.json"
                                    result = json.loads(path.read_text()) if path.exists() else {}
                                    result.update(status="FAIL", failure=repr(error), traceback=traceback.format_exc())
                                result.update(version=version, payload_bytes=payload)
                                (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
                                # All offers remain in per-case files. The index keeps no growing offer history.
                                row = dict(result)
                                if "client" in row:
                                    row["client"] = {key: value for key, value in row["client"].items() if key != "offers"}
                                rows.append(row)
                                print(len(rows), "/", planned, version, transport, workload, strategy, payload, seed, result["status"], flush=True)
    finally:
        groups = {}
        for row in rows:
            key = (row["version"], row["transport"], row["workload"], row["strategy"], row["payload_bytes"])
            groups.setdefault(key, []).append(row)
        summary = []
        for key, values in sorted(groups.items()):
            group = dict(zip(["version", "transport", "workload", "strategy", "payload_bytes"], key))
            group.update(cases=len(values), statuses=[r["status"] for r in values])
            if all("client" in r for r in values):
                metrics = {
                    "requests_per_second": [r["client"]["completed"] * 1e9 / r["client"]["elapsed_ns"] for r in values],
                    "p95_ms": [r["client"]["P95"] / 1e6 for r in values],
                    "p99_ms": [r["client"]["P99"] / 1e6 for r in values],
                    "triple_cpu_seconds": [r["process_triple_cpu_seconds"] for r in values],
                }
                group["metrics"] = {name: {"median": statistics.median(samples), "min": min(samples), "max": max(samples), "samples": samples} for name, samples in metrics.items()}
                group["offered"] = sum(r["client"]["offered"] for r in values)
                group["completed"] = sum(r["client"]["completed"] for r in values)
            summary.append(group)
        metadata["load_average_at_end"] = os.getloadavg()
        (output / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
        (output / "results.json").write_text(json.dumps(rows, indent=2) + "\n")
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    if len(rows) != planned or any(r["status"] == "FAIL" for r in rows):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
