#!/usr/bin/env python3
"""Accept a candidate through a real local Go module proxy, without replace."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MODULE = "github.com/KanaDoodle/KanaRPC"
ZIP_TOOL = '''package main
import("os";"fmt";"golang.org/x/mod/module";"golang.org/x/mod/zip")
func main(){f,e:=os.Create(os.Args[3]);if e!=nil{panic(e)};defer f.Close();if e=zip.CreateFromDir(f,module.Version{Path:"github.com/KanaDoodle/KanaRPC",Version:os.Args[2]},os.Args[1]);e!=nil{panic(e)};p,e:=module.EscapePath("github.com/KanaDoodle/KanaRPC");if e!=nil{panic(e)};fmt.Println(p)}
'''


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--version", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOWORK="off")
    result = {"version": args.version, "scope": "local file module proxy and isolated module cache; no public version published", "commands": []}

    def run(name, command, cwd, extra=None):
        p = subprocess.run(command, cwd=cwd, env=dict(env, **(extra or {})), capture_output=True, text=True, timeout=240)
        (output / (name + ".txt")).write_text(p.stdout + p.stderr)
        result["commands"].append({"name": name, "exit": p.returncode})
        if p.returncode: raise RuntimeError(f"{name} failed; see log")
        return p.stdout.strip()

    try:
        stage = output / "source"
        stage.mkdir()
        files = sorted(set(subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard"], cwd=ROOT, text=True).splitlines()))
        for name in files:
            source = ROOT / name
            if source.is_symlink() or not source.is_file(): continue
            # Include real evidence sentinels so the ZIP test verifies Go's
            # nested-module exclusion, rather than merely our staging filter.
            if name.startswith("docs/verification/") and name not in ("docs/verification/go.mod", "docs/verification/README.md", "docs/verification/protobuf-r11/manifest.json"): continue
            if name.startswith("tools/lifecycle-compare/"): continue
            destination = stage / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        tool = output / "zip-tool"
        tool.mkdir()
        (tool / "go.mod").write_text("module example.com/module-packaging\n\ngo 1.26.10\n\nrequire golang.org/x/mod v0.42.0\n")
        (tool / "main.go").write_text(ZIP_TOOL)
        run("zip-tool-tidy", ["go", "mod", "tidy"], tool)
        archive = output / ("kanarpc_" + args.version + ".zip")
        escaped = run("module-zip", ["go", "run", ".", str(stage), args.version, str(archive)], tool)
        with zipfile.ZipFile(archive) as z:
            names = z.namelist()
            if any("/docs/verification/" in n for n in names): raise RuntimeError("evidence leaked into module zip")
            result["module_zip"] = {"bytes": archive.stat().st_size, "entries": len(names), "sha256": hashlib.sha256(archive.read_bytes()).hexdigest()}
        proxy = output / "proxy"
        versions = proxy / escaped / "@v"
        versions.mkdir(parents=True)
        shutil.copyfile(archive, versions / (args.version + ".zip"))
        (versions / (args.version + ".mod")).write_bytes((ROOT / "go.mod").read_bytes())
        (versions / (args.version + ".info")).write_text(json.dumps({"Version": args.version, "Time": "2026-11-01T00:00:00Z"}))
        (versions / "list").write_text(args.version + "\n")
        consumer = output / "consumer"
        consumer.mkdir()
        (consumer / "go.mod").write_text(f"module example.com/versioned-consumer\n\ngo 1.26.10\n\nrequire {MODULE} {args.version}\n")
        shutil.copyfile(ROOT / "cmd/protobuf-demo/main.go", consumer / "main.go")
        (consumer / "observability.go").write_text(
            'package main\nimport _ "github.com/KanaDoodle/KanaRPC/observability"\n'
        )
        # Keep the consumer cache isolated while reading already downloaded,
        # checksum-verified public dependencies as a read-only file proxy.
        # The candidate is always resolved from its own first-priority proxy.
        downloaded = Path(subprocess.check_output(["go", "env", "GOMODCACHE"], cwd=ROOT, env=env, text=True).strip()) / "cache/download"
        result["dependency_proxy"] = downloaded.as_uri()
        isolated = {"GOPROXY": proxy.as_uri() + "," + downloaded.as_uri() + ",https://proxy.golang.org,direct", "GONOSUMDB": MODULE, "GOMODCACHE": str(output / "module-cache"), "GOBIN": str(output / "bin")}
        run("consumer-tidy", ["go", "mod", "tidy"], consumer, isolated)
        selected = json.loads(run("selected-module", ["go", "list", "-m", "-json", MODULE], consumer, isolated))
        if selected.get("Replace") or selected["Version"] != args.version: raise RuntimeError("candidate was not consumed by version")
        run("consumer-run", ["go", "run", "."], consumer, isolated)
        run("consumer-production-run", ["go", "run", "-mod=mod", MODULE + "/cmd/production-demo"], consumer, isolated)
        run("install-generator", ["go", "install", MODULE + "/cmd/protoc-gen-kanarpc@" + args.version], consumer, isolated)
        observed = run("installed-version", [str(output / "bin/protoc-gen-kanarpc"), "--version"], consumer, isolated)
        if args.version not in observed: raise RuntimeError("installed generator version mismatch")
        result["status"] = "PASS"
    except BaseException as error:
        result.update(status="FAIL", failure=repr(error))
        raise
    finally:
        (output / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))


if __name__ == "__main__": main()
