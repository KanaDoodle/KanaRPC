#!/usr/bin/env python3
"""Install pinned, checksum-verified build tools into a new owned directory."""
import argparse
import hashlib
import io
import json
from pathlib import Path, PurePosixPath
import platform
import subprocess
import tarfile
import urllib.request
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--cache", type=Path, help="read existing downloads by asset filename")
    args = parser.parse_args()
    system = {"Linux": "linux", "Darwin": "darwin"}.get(platform.system())
    arch = {"x86_64": "amd64", "arm64": "arm64", "aarch64": "arm64"}.get(platform.machine())
    key = f"{system}-{arch}"
    assets = json.loads((ROOT / "tools/versions.json").read_text())["assets"]
    if key not in assets:
        raise RuntimeError(f"no pinned tool distribution for {key}")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    binary = output / "bin"
    binary.mkdir()
    receipt = {"platform": key, "tools": {}}
    for tool, spec in assets[key].items():
        name = spec["url"].rsplit("/", 1)[1]
        cached = args.cache / name if args.cache else None
        raw = cached.read_bytes() if cached and cached.is_file() else urllib.request.urlopen(spec["url"], timeout=90).read()
        if hashlib.sha256(raw).hexdigest() != spec["sha256"]:
            raise RuntimeError(f"checksum mismatch for {tool}")
        if tool == "buf":
            (binary / tool).write_bytes(raw)
        else:
            if name.endswith(".zip"):
                with zipfile.ZipFile(io.BytesIO(raw)) as archive:
                    members = [(i.filename, archive.read(i)) for i in archive.infolist() if not i.is_dir()]
            else:
                with tarfile.open(fileobj=io.BytesIO(raw)) as archive:
                    members = [(i.name, archive.extractfile(i).read()) for i in archive.getmembers() if i.isfile()]
            for path, body in members:
                path = PurePosixPath(path)
                if path.is_absolute() or ".." in path.parts:
                    raise RuntimeError("unsafe archive path")
                destination = None
                if tool == "etcd" and path.name == "etcd":
                    destination = binary / "etcd"
                elif tool == "protoc":
                    if str(path) == "bin/protoc": destination = binary / "protoc"
                    elif path.parts[0] == "include": destination = output / str(path)
                if destination:
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(body)
        (binary / tool).chmod(0o755)
        version = subprocess.check_output([str(binary / tool), "--version"], text=True).strip()
        receipt["tools"][tool] = dict(spec, observed_version=version)
    (output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
    print(binary)


if __name__ == "__main__": main()
