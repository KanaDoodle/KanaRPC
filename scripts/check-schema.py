#!/usr/bin/env python3
"""Run Buf's binary, JSON and generated-source compatibility policies."""
import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--buf", default=os.environ.get("BUF", "buf"))
    parser.add_argument("--input", default=str(ROOT))
    parser.add_argument("--against", default=str(ROOT / "api/schema.binpb"))
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    if args.output: args.output.mkdir(parents=True, exist_ok=False)
    expected = json.loads((ROOT / "tools/versions.json").read_text())["assets"]["linux-amd64"]["buf"]["version"].lstrip("v")
    version = subprocess.check_output([args.buf, "--version"], text=True).strip()
    if version != expected: raise RuntimeError(f"Buf {expected} required, found {version}")
    result = {"buf": version, "input": args.input, "against": args.against, "checks": {}}
    for category in ("WIRE", "WIRE_JSON", "FILE"):
        config = json.dumps({"version": "v2", "modules": [{"path": ".", "excludes": ["docs/verification", "tools"]}], "breaking": {"use": [category]}})
        command = [args.buf, "breaking", args.input, "--against", args.against, "--config", config, "--error-format=json"]
        p = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        result["checks"][category] = {"exit": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
        if not args.output and p.returncode: print(p.stdout + p.stderr, end="")
    result["status"] = "PASS" if all(c["exit"] == 0 for c in result["checks"].values()) else "FAIL"
    if args.output: (args.output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k:v for k,v in result.items() if k != "checks"}))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__": raise SystemExit(main())
