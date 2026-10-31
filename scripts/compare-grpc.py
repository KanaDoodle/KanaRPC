#!/usr/bin/env python3
"""Serialized paired two-process loopback comparison. Never hides failures."""
import argparse
import gzip
import hashlib
import json
import os
from pathlib import Path
import random
import statistics
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools/grpc-compare"


def hashes(root=ROOT):
    tool = root / "tools/grpc-compare"
    files = list(root.glob("*.go")) + list((root / "internal/wire").glob("*.go")) + list((root / "internal/testcert").glob("*.go")) + list(tool.rglob("*.go")) + list(tool.rglob("*.proto"))
    files += [root/"go.mod",root/"go.sum",tool/"go.mod",tool/"go.sum",root/"scripts/compare-grpc.py"]
    return {str(f.relative_to(root)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(set(files))}


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("--output",type=Path,required=True)
    parser.add_argument("--duration",type=float,default=2)
    parser.add_argument("--repeats",type=int,default=3)
    parser.add_argument("--race",action="store_true")
    parser.add_argument("--smoke",action="store_true")
    parser.add_argument("--focus",action="store_true",help="concurrency 16 only")
    parser.add_argument("--profile",action="store_true",help="diagnostic profiles, never performance evidence")
    parser.add_argument("--baseline-root",type=Path,help="frozen Kana source, measured as a third variant")
    parser.add_argument("--long-stream",action="store_true",help="also compare streams spanning multiple credit windows")
    args=parser.parse_args()
    if args.focus and args.smoke:parser.error("--focus and --smoke select disjoint concurrency levels")
    if args.duration<=0 or args.repeats<1:parser.error("positive duration and repetitions required")
    output=args.output.resolve();output.mkdir(parents=True,exist_ok=False)
    env=dict(os.environ,GOWORK="off",GOMAXPROCS="4",GOTOOLCHAIN="go1.27.3")
    before=hashes();binary=output/"compare"
    build=["go","build","-mod=readonly","-o",str(binary)]
    if args.race:build.append("-race")
    build.append(".")
    with (output/"build.txt").open("wb") as log:subprocess.run(build,cwd=TOOL,env=env,stdout=log,stderr=log,check=True)
    baseline = args.baseline_root.resolve() if args.baseline_root else None
    baseline_before = hashes(baseline) if baseline else None
    baseline_binary = output/"baseline"
    if baseline:
        harness=[p for p in TOOL.rglob("*") if p.is_file() and (p.suffix in [".go",".proto"] or p.name in ["go.mod","go.sum"])]
        for source in harness:
            if source.read_bytes()!=(baseline/source.relative_to(ROOT)).read_bytes():
                raise RuntimeError("comparison harness differs: "+str(source.relative_to(ROOT)))
        base_build=["go","build","-mod=readonly","-o",str(baseline_binary)]
        if args.race:base_build.append("-race")
        with (output/"baseline-build.txt").open("wb") as log:
            subprocess.run(base_build+["."],cwd=baseline/"tools/grpc-compare",env=env,stdout=log,stderr=log,check=True)
    metadata={"scope":"separate processes, serial closed-loop loopback; one persistent connection; no compression, discovery or governance; configured retries disabled, grpc transparent transport retries unchanged; per-RPC deadline 2s, limits 128, 64 warmups; not production/WAN/inference performance","args":vars(args)|{"output":str(output),"baseline_root":str(baseline) if baseline else None},"baseline_source_sha256":baseline_before,"baseline_binary_sha256":hashlib.sha256(baseline_binary.read_bytes()).hexdigest() if baseline else None,"source_sha256":before,"go":subprocess.check_output(["go","version"],cwd=TOOL,env=env,text=True).strip(),"modules":subprocess.check_output(["go","list","-m","all"],cwd=TOOL,env=env,text=True),"GOMAXPROCS":4,"binary_sha256":hashlib.sha256(binary.read_bytes()).hexdigest()}
    (output/"manifest.json").write_text(json.dumps(metadata,indent=2)+"\n")
    results=[];cases=[(t,size,c,chunks) for t in ["tcp","tls"] for size,chunks in ([(64,1),(4096,1),(1024,8)] + ([(1024,64)] if args.long_stream else [])) for c in ([1] if args.smoke else [1,16])]
    random.Random(20261101).shuffle(cases)
    if args.focus:cases=[c for c in cases if c[2]==16]
    try:
        for rep in range(args.repeats):
            for index,(transport,size,concurrency,chunks) in enumerate(cases):
                order=["baseline","kana","grpc"] if baseline else ["kana","grpc"]
                offset=(rep+index)%len(order);order=order[offset:]+order[:offset]
                if rep%2:order=order[::-1]
                for variant in order:
                    engine="kana" if variant=="baseline" else variant
                    executable=baseline_binary if variant=="baseline" else binary
                    name=f"r{rep}_{transport}_b{size}_c{concurrency}_n{chunks}_{variant}";folder=output/name;folder.mkdir();ready=folder/"ready.json";serverout=folder/"server.json"
                    options=["-engine",engine,"-transport",transport,"-ready",str(ready)]
                    with (folder/"server.stderr").open("wb") as stderr:
                        server_env=dict(env)
                        client_env=dict(env)
                        if args.profile:
                            server_env['KANARPC_COMPARE_PROFILE']=str(folder/'server')
                            client_env['KANARPC_COMPARE_PROFILE']=str(folder/'client')
                        server=subprocess.Popen([str(executable),"-mode","server",*options,"-out",str(serverout)],env=server_env,stdout=stderr,stderr=stderr)
                        owned=server.pid
                        try:
                            until=time.monotonic()+10
                            while not ready.exists():
                                if server.poll() is not None:raise RuntimeError(f"server exited: {name}")
                                if time.monotonic()>until:raise TimeoutError(name)
                                time.sleep(.01)
                            with (folder/"client.stderr").open("wb") as log:
                                client=subprocess.run([str(executable),"-mode","client",*options,"-out",str(folder/"client.json"),"-size",str(size),"-workers",str(concurrency),"-chunks",str(chunks),"-duration",str(args.duration)+"s"],env=client_env,stdout=log,stderr=log,timeout=args.duration+30)
                            if client.returncode:raise RuntimeError(f"client failed: {name}")
                            row=json.loads((folder/"client.json").read_text());row["run"]=name;row["variant"]=variant
                            results.append({k:v for k,v in row.items() if k!="offers"})
                            with gzip.open(folder/"client.json.gz","wt") as compressed:json.dump(row,compressed)
                            (folder/"client.json").unlink()
                            info=json.loads(ready.read_text())
                            with urllib.request.urlopen(urllib.request.Request(info["Admin"]+"/stop",data=b""),timeout=5) as response:response.read()
                            server.wait(timeout=8)
                            if server.returncode or json.loads(serverout.read_text())["Active"]!=0:raise RuntimeError(f"server did not settle: {name}")
                            if engine=="kana":
                                report=json.loads((folder/"server.json.drain.json").read_text())
                                if not report["Quiescent"] or any(report["TotalFaults"].values()):raise RuntimeError(f"delivery/drain failure: {name}")
                        finally:
                            if server.poll() is None:server.terminate();server.wait(timeout=5)
                            (folder/"ownership.json").write_text(json.dumps({"owned_pid":owned,"reaped":server.poll() is not None})+"\n")
                    print(name,"ok",flush=True)
        if baseline and hashes(baseline)!=baseline_before:raise RuntimeError("baseline source changed during measurement")
        if hashes()!=before:raise RuntimeError("source changed during measurement")
        (output/"result.json").write_text(json.dumps({"status":"PASS","runs":results},indent=2)+"\n")
    except BaseException as e:
        (output/"result.json").write_text(json.dumps({"status":"FAIL","failure":repr(e),"runs":results},indent=2)+"\n");raise


if __name__=="__main__":main()
