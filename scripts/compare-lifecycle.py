#!/usr/bin/env python3
"""Serial behavior controls, not a speedup benchmark; requires the old sibling."""
import argparse
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.repeats <= 100:
        parser.error("repeats must be 1..100")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOWORK="off")
    binary = output / "lifecycle-compare"
    subprocess.run(["go", "build", "-o", str(binary), "."], cwd=ROOT / "tools/lifecycle-compare", env=env, check=True)
    records = []
    for repeat in range(1, args.repeats + 1):
        for implementation in ["old-legacy", "old-cooperative", "new-v2"]:
            for scenario in ["cancel", "drain"]:
                process = subprocess.run([str(binary), "-implementation", implementation, "-scenario", scenario], text=True, capture_output=True, timeout=15)
                name = f"{repeat}-{implementation}-{scenario}"
                (output / f"{name}.stdout").write_text(process.stdout)
                (output / f"{name}.stderr").write_text(process.stderr)
                try:
                    row = json.loads(process.stdout)
                except ValueError:
                    row = {"failure": "invalid process output"}
                row.update(repeat=repeat, implementation=implementation, scenario=scenario, exit_code=process.returncode)
                records.append(row)
                (output / "comparison.jsonl").write_text("".join(json.dumps(item, ensure_ascii=False) + "\n" for item in records))
                if process.returncode or "failure" in row:
                    raise RuntimeError(f"comparison failed: {name}")
    print(json.dumps({"status": "PASS", "cases": len(records), "scope": "behavior only; no inference or performance claim"}))


if __name__ == "__main__":
    main()
