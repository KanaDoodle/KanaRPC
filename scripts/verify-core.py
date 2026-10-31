#!/usr/bin/env python3
"""Run owned server/client processes; SIGTERM during an admitted stream."""
import argparse
import json
import os
from pathlib import Path
import selectors
import signal
import subprocess
import time

ROOT = Path(__file__).resolve().parents[1]


def line_with_timeout(process, timeout=10):
    # Binary, unbuffered pipes avoid a TextIO buffer hiding ready lines.
    data = bytearray()
    with selectors.DefaultSelector() as selector:
        selector.register(process.stdout, selectors.EVENT_READ)
        until = time.monotonic() + timeout
        while time.monotonic() < until:
            if not selector.select(max(0, until - time.monotonic())):
                break
            char = os.read(process.stdout.fileno(), 1)
            if not char:
                raise RuntimeError("process ended before lifecycle record")
            data.extend(char)
            if char == b"\n":
                return json.loads(data)
    raise TimeoutError("lifecycle record timed out")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    env = dict(os.environ, GOWORK="off")
    binary = output / "lifecycle-demo"
    subprocess.run(["go", "build", "-o", str(binary), "./cmd/lifecycle-demo"], cwd=ROOT, env=env, check=True)
    processes, logs = [], []
    records = {}
    try:
        server_log = (output / "server.stderr").open("wb")
        logs.append(server_log)
        server = subprocess.Popen([str(binary), "-mode", "server"], stdout=subprocess.PIPE, stderr=server_log, bufsize=0)
        processes.append(server)
        records["server_ready"] = line_with_timeout(server)
        client_log = (output / "client.stderr").open("wb")
        logs.append(client_log)
        client = subprocess.Popen([str(binary), "-mode", "client", "-address", records["server_ready"]["address"]], stdout=subprocess.PIPE, stderr=client_log, bufsize=0)
        processes.append(client)
        records["client_drain_ready"] = line_with_timeout(client)
        if records["client_drain_ready"]["phase"] != "drain-ready":
            raise RuntimeError("client did not reach admitted stream")
        server.send_signal(signal.SIGTERM)
        records["client"] = line_with_timeout(client)
        records["server_drain_starting"] = line_with_timeout(server)
        records["server"] = line_with_timeout(server)
        for process in processes:
            if process.wait(timeout=5) != 0:
                raise RuntimeError("process exited unsuccessfully")
        stats = records["server"]["stats"]
        assert records["client"]["phase"] == "passed"
        assert records["client"]["chunks"] == 20 and records["client"]["remote_canceled"] == 1
        assert records["client"]["drain_eof"] is True
        assert records["server_drain_starting"]["phase"] == "drain-starting"
        assert records["server_drain_starting"]["drain_handler_active"] is True
        assert records["server_drain_starting"]["stats"]["InFlight"] >= 1
        assert records["client"]["stats"]["Pending"] == 0
        assert records["client"]["stats"]["InFlightBufferedChunks"] == 0
        assert all(stats[key] == 0 for key in ["Connections", "InFlight", "Executing", "LostTerminals"])
        assert records["server"]["error"] == ""
        records["status"] = "PASS"
        records["scope"] = "real loopback transport and process signal; integer/timer fixture, no model or performance claim"
    except BaseException as error:
        records["status"] = "FAIL"
        records["failure"] = repr(error)
        raise
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=3)
            if process.stdout:
                process.stdout.close()
        for log in logs:
            log.close()
        (output / "result.json").write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps(records, ensure_ascii=False))


if __name__ == "__main__":
    main()
