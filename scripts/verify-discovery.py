#!/usr/bin/env python3
"""Own one isolated etcd process, run Go checks, and always reap only that PID."""
import argparse
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--toolchain", default="local")
    parser.add_argument("--race", action="store_true")
    parser.add_argument("--count", type=int, default=1)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    executable = shutil.which("etcd")
    if not executable:
        raise RuntimeError("etcd executable required; no fixture substitutes for etcd")
    client_port, peer_port = port(), port()
    while peer_port == client_port:
        peer_port = port()
    client, peer = f"http://127.0.0.1:{client_port}", f"http://127.0.0.1:{peer_port}"
    member_name = f"kanarpc-r7b-{os.getpid()}"
    command = [executable, "--name", member_name, "--data-dir", str(output / "data"), "--listen-client-urls", client, "--advertise-client-urls", client, "--listen-peer-urls", peer, "--initial-advertise-peer-urls", peer, "--initial-cluster", f"{member_name}={peer}"]
    result = {"endpoint": client, "etcd_version": subprocess.check_output([executable, "--version"], text=True), "scope": "owned single-member etcd; real lease, watch, compaction and TCP gate; not a quorum/production test"}
    with (output / "etcd.log").open("wb") as log:
        process = subprocess.Popen(command, stdout=log, stderr=log)
        result["owned_pid"] = process.pid
        try:
            until = time.monotonic() + 15
            while time.monotonic() < until:
                if process.poll() is not None:
                    raise RuntimeError("owned etcd exited during startup")
                try:
                    with urllib.request.urlopen(client + "/health", timeout=.3) as response:
                        if json.load(response).get("health") == "true":
                            break
                except (OSError, ValueError):
                    pass
                time.sleep(.05)
            else:
                raise TimeoutError("owned etcd health check timed out")
            request = urllib.request.Request(client + "/v3/cluster/member/list", data=b"{}", headers={"Content-Type": "application/json"})
            with urllib.request.urlopen(request, timeout=2) as response:
                membership = json.load(response)
            members = membership.get("members", [])
            if process.poll() is not None or len(members) != 1 or members[0].get("name") != member_name or members[0].get("peerURLs") != [peer] or members[0].get("clientURLs") != [client]:
                raise RuntimeError("endpoint ownership not established; refusing compaction tests")
            result["owned_membership"] = membership
            env = dict(os.environ, GOWORK="off", KANARPC_ETCD_ENDPOINT=client)
            if args.toolchain != "local":
                env["GOTOOLCHAIN"] = args.toolchain
            test = ["go", "test", "-count=" + str(args.count), "-timeout=120s", "-json"]
            if args.race:
                test.append("-race")
            test.append("./...")
            result["command"] = test
            with (output / "test.jsonl").open("wb") as stdout, (output / "test.stderr").open("wb") as stderr:
                test_result = subprocess.run(test, cwd=ROOT, env=env, stdout=stdout, stderr=stderr, timeout=180)
            result["test_exit_code"] = test_result.returncode
            rows = [json.loads(line) for line in (output / "test.jsonl").read_text().splitlines()]
            result["checks"] = {key: sum(row.get("Action") == key and bool(row.get("Test")) for row in rows) for key in ["pass", "skip", "fail"]}
            if test_result.returncode or result["checks"]["fail"] or result["checks"]["skip"]:
                raise RuntimeError("Go checks failed or required real-etcd tests skipped")
            result["status"] = "PASS"
        except BaseException as error:
            result["status"] = "FAIL"
            result["failure"] = repr(error)
            raise
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=8)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            result["owned_process_reaped"] = process.poll() is not None
            (output / "result.json").write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
