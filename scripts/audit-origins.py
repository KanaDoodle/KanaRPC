#!/usr/bin/env python3
"""Record reproducible source/dependency observations, not originality proof."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def run(arguments, root=ROOT):
    return subprocess.check_output(arguments, cwd=root, env=dict(os.environ, GOWORK="off"))


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--legacy", type=Path, default=ROOT.parent / "KanaRPC-Go")
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    legacy = args.legacy.resolve()
    reference = run(["git", "rev-parse", "refs/remotes/upstream/main"], legacy).decode().strip()
    old_paths = run(["git", "ls-tree", "-r", "--name-only", reference], legacy).decode().splitlines()
    upstream, observations = {}, []
    for name in old_paths:
        if not name.endswith(".go"):
            continue
        data = run(["git", "show", f"{reference}:{name}"], legacy)
        upstream[name] = digest(data)
        current = legacy / name
        current_hash = digest(current.read_bytes()) if current.is_file() else None
        observations.append({"path": name, "upstream_sha256": upstream[name], "legacy_current_sha256": current_hash, "unchanged": current_hash == upstream[name]})
    old_hashes = {digest(p.read_bytes()): str(p.relative_to(legacy)) for p in legacy.rglob("*.go") if not any(part in {"vendor", ".git"} for part in p.parts)}
    manifest, identical = {}, []
    for path in sorted(ROOT.rglob("*")):
        if not path.is_file() or any(part in {".git", "__pycache__"} for part in path.parts):
            continue
        relative = path.relative_to(ROOT)
        if relative.parts[:2] == ("docs", "verification"):
            continue
        if path.suffix not in {".go", ".py", ".md"} and path.name not in {"go.mod", "go.sum", "LICENSE", ".gitignore"}:
            continue
        value = digest(path.read_bytes())
        manifest[str(relative)] = value
        if path.suffix == ".go" and value in old_hashes:
            identical.append({"new": str(relative), "legacy": old_hashes[value]})
    dependencies = [name for name in run(["go", "list", "-deps", "-f", "{{if not .Standard}}{{.ImportPath}}{{end}}", "./..."]).decode().splitlines() if name]
    # The root ./... pattern skips the explicitly separate comparison module.
    external = [name for name in dependencies if name and name != "github.com/KanaDoodle/KanaRPC" and not name.startswith("github.com/KanaDoodle/KanaRPC/")]
    # R7B explicitly adopts quic-go and the etcd client, never a legacy core.
    direct = [name for name in run(["go", "list", "-m", "-f", "{{if not .Indirect}}{{.Path}}{{end}}", "all"]).decode().splitlines() if name]
    allowed = {"github.com/KanaDoodle/KanaRPC", "github.com/quic-go/quic-go", "go.etcd.io/etcd/client/v3", "go.etcd.io/etcd/api/v3"}
    forbidden = [name for name in direct if name not in allowed]
    forbidden += [name for name in dependencies if name.startswith("github.com/KanaDoodle/KanaRPC-Go") or name.startswith("github.com/youngyangyang04/KamaRPC-Go")]
    result = {
        "captured_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "new_module": "github.com/KanaDoodle/KanaRPC",
        "go_version": run(["go", "version"]).decode().strip(),
        "legacy_head": run(["git", "rev-parse", "HEAD"], legacy).decode().strip(),
        "local_upstream_ref": reference,
        "ref_scope": "local tracking ref, not asserted to be current remote HEAD",
        "legacy_status": run(["git", "status", "--short"], legacy).decode().splitlines(),
        "legacy_upstream_files": observations,
        "new_source_sha256": manifest,
        "root_nonstandard_packages": dependencies,
        "adopted_third_party_packages": external,
        "direct_modules": direct,
        "unexpected_root_dependencies": forbidden,
        "whole_go_file_matches_against_legacy": identical,
        "license": "AGPL-3.0 text copied from legacy LICENSE; implementation source was newly written",
        "limits": "file hashes and direct dependency checks do not prove legal independence, strict clean-room development, novelty, or complete absence of source similarity",
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    if forbidden or identical:
        raise SystemExit("source boundary check requires review")
    print(json.dumps({"status": "PASS", "source_files": len(manifest), "unexpected_root_dependencies": forbidden, "whole_file_matches": identical}))


if __name__ == "__main__":
    main()
