#!/usr/bin/env python3
"""Exercise real Buf policies on compatible additions and breaking mutations."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--buf", default=os.environ.get("BUF", "buf"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    original = (ROOT / "examples/echo/echo.proto").read_text()
    cases = {
        "unchanged": original,
        "add_field": original.replace("reserved 6, 7;", "reserved 6, 7; uint64 created_at = 8;"),
        "add_method": original.replace("service EchoService {", "service EchoService { rpc Added(EchoRequest) returns (EchoResponse);"),
        "rename_field": original.replace("string text = 1;", "string body = 1;", 1),
        "change_type": original.replace("string text = 1;", "int64 text = 1;", 1),
        "remove_presence": original.replace("optional string label = 3;", "string label = 3;"),
        "change_go_package": original.replace("KanaRPC/examples/echo", "KanaRPC/examples/other"),
        "remove_field_reserved": original.replace("string text = 1;", 'reserved 1; reserved "text";', 1),
        "change_stream": original.replace("returns (stream EchoResponse)", "returns (EchoResponse)"),
        "reuse_number": original.replace("string text = 1;", "string different = 1;", 1),
        "reuse_reserved": original.replace("reserved 6, 7;", "reserved 6, 7; string conflict = 6;"),
    }
    results = {}
    for name, schema in cases.items():
        fixture = output / name
        (fixture / "examples/echo").mkdir(parents=True)
        (fixture / "examples/echo/echo.proto").write_text(schema)
        command = [sys.executable, str(ROOT / "scripts/check-schema.py"), "--buf", args.buf, "--input", str(fixture), "--output", str(fixture / "result")]
        p = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
        (fixture / "command.txt").write_text(p.stdout + p.stderr)
        result = json.loads((fixture / "result/result.json").read_text())
        passed = name in ("unchanged", "add_field", "add_method")
        if (p.returncode == 0) != passed: raise RuntimeError(f"unexpected policy result: {name}")
        results[name] = {category: "PASS" if r["exit"] == 0 else "REJECT" for category,r in result["checks"].items()}
    (output / "result.json").write_text(json.dumps({"status": "PASS", "cases": results}, indent=2) + "\n")
    print(json.dumps(results))


if __name__ == "__main__": main()
