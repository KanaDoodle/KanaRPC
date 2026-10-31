#!/usr/bin/env python3
"""Verify a candidate and build SDK artifacts without tagging or publishing."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile

ROOT = Path(__file__).resolve().parents[1]


def source_hashes():
    names = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, text=True).splitlines()
    result = {}
    for name in sorted(set(names)):
        if name.startswith("docs/verification/") and name != "docs/verification/go.mod": continue
        if name.startswith("tools/lifecycle-compare/"): continue
        p = ROOT / name
        if p.is_file() and not p.is_symlink():
            result[name] = hashlib.sha256(p.read_bytes()).hexdigest()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--buf", default=os.environ.get("BUF", "buf"))
    parser.add_argument("--protoc", default=os.environ.get("PROTOC", "protoc"))
    parser.add_argument("--against", default=str(ROOT / "api/schema.binpb"))
    parser.add_argument("--require-clean", action="store_true")
    args = parser.parse_args()
    # v0 is intentional until the SDK public API receives a stable v1 contract.
    if not re.fullmatch(r"v0\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(?:rc|beta)\.[1-9]\d*)?", args.version):
        parser.error("use a v0 semantic version, for example v0.1.0 or v0.1.0-rc.1")
    output = args.output.resolve()
    if output == ROOT or ROOT in output.parents: parser.error("release output must be outside the checkout")
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT))
    if args.require_clean and dirty: parser.error("release requires a clean committed checkout")
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOWORK="off")
    result = {"version": args.version, "scope": "local candidate verification and packaging; no Git tag, push or GitHub release", "commands": []}

    def run(name, command, extra=None):
        with (output / (name + ".txt")).open("wb") as log:
            p = subprocess.run(command, cwd=ROOT, env=dict(env, **(extra or {})), stdout=log, stderr=log, timeout=240)
        result["commands"].append({"name": name, "command": command, "exit": p.returncode})
        if p.returncode: raise RuntimeError(f"{name} failed; see {name}.txt")

    try:
        initial = source_hashes()
        result["source_sha256"] = initial
        result["git_commit"] = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip()
        result["worktree_dirty"] = dirty
        run("build", ["go", "build", "-mod=readonly", "./..."])
        run("vet", ["go", "vet", "./..."])
        run("schema", [sys.executable, str(ROOT / "scripts/check-schema.py"), "--buf", args.buf, "--against", args.against, "--output", str(output / "schema")])
        run("protobuf-sdk", [sys.executable, str(ROOT / "scripts/verify-protobuf.py"), "--protoc", args.protoc, "--output", str(output / "sdk")])
        # Required etcd integration is never silently replaced by skipped tests.
        run("race", [sys.executable, str(ROOT / "scripts/verify-discovery.py"), "--race", "--output", str(output / "race")])
        run("versioned-module", [sys.executable, str(ROOT / "scripts/verify-module.py"), "--version", args.version, "--output", str(output / "module")])
        run("schema-image", [args.buf, "build", "--as-file-descriptor-set", "-o", str(output / "schema.binpb")])
        artifacts = {}
        module_zip = next((output / "module").glob("kanarpc_*.zip"))
        (output / module_zip.name).write_bytes(module_zip.read_bytes())
        artifacts[module_zip.name] = hashlib.sha256(module_zip.read_bytes()).hexdigest()
        for system, arch in [("linux", "amd64"), ("darwin", "arm64")]:
            folder = output / (system + "-" + arch)
            folder.mkdir()
            binary = folder / "protoc-gen-kanarpc"
            run("build-" + system, ["go", "build", "-trimpath", "-buildvcs=false", "-ldflags=-X main.buildVersion=" + args.version, "-o", str(binary), "./cmd/protoc-gen-kanarpc"], {"GOOS": system, "GOARCH": arch, "CGO_ENABLED": "0"})
            name = f"protoc-gen-kanarpc_{args.version}_{system}_{arch}.tar.gz"
            with tarfile.open(output / name, "w:gz") as archive:
                archive.add(binary, arcname="protoc-gen-kanarpc")
                for notice in ("LICENSE", "ORIGINS.md", "THIRD_PARTY_NOTICES.md"):
                    archive.add(ROOT / notice, arcname=notice)
            artifacts[name] = hashlib.sha256((output / name).read_bytes()).hexdigest()
        artifacts["schema.binpb"] = hashlib.sha256((output / "schema.binpb").read_bytes()).hexdigest()
        (output / "SHA256SUMS").write_text("".join(f"{sha}  {name}\n" for name,sha in sorted(artifacts.items())))
        if source_hashes() != initial: raise RuntimeError("source changed during release verification")
        result.update(status="PASS", artifacts=artifacts)
    except BaseException as error:
        result.update(status="FAIL", failure=repr(error))
        raise
    finally:
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__": main()
