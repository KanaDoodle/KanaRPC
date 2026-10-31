#!/usr/bin/env python3
"""Compile/run a public API consumer in an independent module, never in go.work."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(); output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    consumer = output/"consumer"; consumer.mkdir()
    (consumer/"go.mod").write_text("module example.com/independent-consumer\n\ngo 1.26.0\n\nrequire github.com/KanaDoodle/KanaRPC v0.0.0\n\nreplace github.com/KanaDoodle/KanaRPC => "+json.dumps(str(ROOT))+"\n")
    source = ROOT/"routing/example_test.go"
    shutil.copy2(source, consumer/"example_test.go")
    env = dict(os.environ, GOWORK="off")
    result = dict(scope="independent local module replacement, public API only, real TCP echo/stream; not a published version or model test", example_sha256=hashlib.sha256(source.read_bytes()).hexdigest())
    try:
        with (output/"tidy.txt").open("wb") as log:
            subprocess.run(["go","mod","tidy"],cwd=consumer,env=env,stdout=log,stderr=log,check=True)
        with (output/"test.jsonl").open("wb") as log:
            subprocess.run(["go","test","-race","-count=3","-json","./..."],cwd=consumer,env=env,stdout=log,stderr=log,check=True)
        result["status"]="PASS"
    except BaseException as error:
        result.update(status="FAIL",failure=repr(error));raise
    finally:(output/"result.json").write_text(json.dumps(result,indent=2)+"\n")
    print(json.dumps(result))


if __name__=="__main__":main()
