#!/usr/bin/env python3
"""Build comparison descriptors from trusted Git history, not changed baselines."""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess

ROOT = Path(__file__).resolve().parents[1]
VERSION = re.compile(r"v0\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(rc|beta)\.([1-9]\d*))?")


def key(tag):
    m = VERSION.fullmatch(tag)
    if not m: return None
    minor, patch, phase, n = m.groups()
    return int(minor), int(patch), {None: 3, "rc": 2, "beta": 1}[phase], int(n or 0)


def main():
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--ref", help="target branch commit SHA for a pull request")
    group.add_argument("--release", help="select preceding v0 tag")
    parser.add_argument("--buf", default=os.environ.get("BUF", "buf"))
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    selected = args.ref
    if selected and not re.fullmatch(r"[0-9a-f]{40}", selected): parser.error("--ref must be a full commit SHA")
    if args.release:
        if key(args.release) is None: parser.error("unsupported release version")
        tags = subprocess.check_output(["git", "tag", "--list", "v0.*"], cwd=ROOT, text=True).splitlines()
        candidates = [t for t in tags if key(t) is not None and key(t) < key(args.release)]
        selected = max(candidates, key=key) if candidates else None
    result = {"selected_ref": selected, "initial_schema": True}
    descriptor = output / "schema.binpb"
    if selected:
        commit = subprocess.check_output(["git", "rev-parse", "--verify", selected + "^{commit}"], cwd=ROOT, text=True).strip()
        result["commit"] = commit
        files = subprocess.check_output(["git", "ls-tree", "-r", "--name-only", commit], cwd=ROOT, text=True).splitlines()
        protos = [f for f in files if f.endswith(".proto") and not f.startswith(("docs/verification/", "tools/"))]
        if protos:
            source = output / "source"
            source.mkdir()
            for name in protos + [f for f in ("buf.yaml", "buf.lock") if f in files]:
                path = PurePosixPath(name)
                if path.is_absolute() or ".." in path.parts: raise RuntimeError("invalid Git source path")
                destination = source / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes(subprocess.check_output(["git", "show", commit + ":" + name], cwd=ROOT))
            subprocess.run([args.buf, "build", str(source), "--as-file-descriptor-set", "-o", str(descriptor)], cwd=ROOT, check=True)
            result["initial_schema"] = False
    if result["initial_schema"]: descriptor.write_bytes((ROOT / "api/schema.binpb").read_bytes())
    result["sha256"] = hashlib.sha256(descriptor.read_bytes()).hexdigest()
    (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__": main()
