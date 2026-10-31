#!/usr/bin/env python3
"""Owned real-etcd, separate-process production-chain acceptance."""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import time
import urllib.request

ROOT=Path(__file__).resolve().parents[1]
def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def hashes():
    return {str(f.relative_to(ROOT)):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted(ROOT.rglob('*.go')) if not any(p in f.relative_to(ROOT).parts for p in ['docs','tools','.git'])}|{f:hashlib.sha256((ROOT/f).read_bytes()).hexdigest() for f in ['go.mod','go.sum','scripts/verify-production.py']}
def request(url,data=None):
    with urllib.request.urlopen(urllib.request.Request(url,data=json.dumps(data).encode() if data is not None else None,headers={'Content-Type':'application/json'}),timeout=2) as response:return response.read()
def main():
    parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);parser.add_argument('--race',action='store_true');args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    etcd=shutil.which('etcd')
    if not etcd:raise RuntimeError('real etcd executable required')
    env=dict(os.environ,GOWORK='off',GOTOOLCHAIN='go1.27.3',KANARPC_DEMO_TOKEN=secrets.token_hex(32),KANARPC_BAD_TOKEN='invalid-fixture-token')
    before=hashes();metadata={'status':'RUNNING','scope':'owned single-member etcd; independent RPC server/client and CLI; real loopback TCP/TLS/QUIC; local OTel SDK; not WAN, model inference or production capacity','source_sha256':before,'race':args.race,'go':subprocess.check_output(['go','version'],cwd=ROOT,env=env,text=True).strip(),'modules':subprocess.check_output(['go','list','-m','all'],cwd=ROOT,env=env,text=True),'runs':[]}
    def command(cmd,folder,name,extra_env=None,expect=0):
        merged=dict(env);merged.update(extra_env or {})
        with (folder/(name+'.stdout')).open('wb') as stdout,(folder/(name+'.stderr')).open('wb') as stderr:p=subprocess.run(cmd,cwd=ROOT,env=merged,stdout=stdout,stderr=stderr,timeout=45)
        row={'name':name,'command':cmd,'exit_code':p.returncode};metadata['runs'].append(row)
        if (p.returncode!=0 if expect==0 else p.returncode==0):raise RuntimeError(f'{name} unexpected exit {p.returncode}')
        return (folder/(name+'.stdout')).read_text()
    demo=out/'production-demo';cli=out/'kanarpc'
    try:
        for binary,pkg in [(demo,'./cmd/production-demo'),(cli,'./cmd/kanarpc')]:command(['go','build','-mod=readonly',*(['-race'] if args.race else []),'-o',str(binary),pkg],out,binary.name+'-build')
        cp,pp=port(),port()
        while cp==pp:pp=port()
        endpoint=f'http://127.0.0.1:{cp}';peer=f'http://127.0.0.1:{pp}';member=f'kanarpc-r15-{os.getpid()}'
        log=(out/'etcd.log').open('wb');process=subprocess.Popen([etcd,'--name',member,'--data-dir',str(out/'etcd-data'),'--listen-client-urls',endpoint,'--advertise-client-urls',endpoint,'--listen-peer-urls',peer,'--initial-advertise-peer-urls',peer,'--initial-cluster',member+'='+peer],stdout=log,stderr=log)
        metadata['etcd']={'owned_pid':process.pid,'version':subprocess.check_output([etcd,'--version'],text=True),'endpoint':endpoint}
        try:
            until=time.monotonic()+15
            while True:
                if process.poll() is not None:raise RuntimeError('owned etcd exited')
                try:
                    if json.loads(request(endpoint+'/health'))['health']=='true':break
                except (OSError,ValueError):pass
                if time.monotonic()>until:raise TimeoutError('etcd health')
                time.sleep(.05)
            members=json.loads(request(endpoint+'/v3/cluster/member/list',{}))['members'];assert len(members)==1 and members[0]['name']==member and members[0]['clientURLs']==[endpoint];metadata['etcd']['membership']=members
            for transport in ['tcp','tls','quic']:
                folder=out/transport;folder.mkdir();ready=folder/'ready.json';namespace=f'/kanarpc/r15/acceptance-{os.getpid()}-{transport}';serverlog=(folder/'server.stderr').open('wb');server=subprocess.Popen([str(demo),'-mode','server','-transport',transport,'-ready',str(ready),'-out',str(folder/'server.json'),'-etcd',endpoint,'-namespace',namespace],env=env,cwd=ROOT,stdout=serverlog,stderr=serverlog)
                try:
                    until=time.monotonic()+15
                    while not ready.exists():
                        if server.poll() is not None:raise RuntimeError(transport+' server exited')
                        if time.monotonic()>until:raise TimeoutError('server readiness')
                        time.sleep(.01)
                    info=json.loads(ready.read_text());shutil.copy2(info['CA'],folder/'ca.pem')
                    command([str(demo),'-mode','client','-ready',str(ready),'-out',str(folder/'client.json'),'-etcd',endpoint,'-namespace',namespace],folder,'client')
                    client=json.loads((folder/'client.json').read_text());assert client['passed'] and client['discovery_etcd'] and client['captured_spans']>=5 and client['trailers']['request-id']=='complete-chain'
                    common=[str(cli),'-address',info['Endpoint']['address'],'-transport',transport,'-token-env','KANARPC_DEMO_TOKEN','-gzip']
                    if transport!='tcp':common+=['-ca',info['CA'],'-server-name','localhost']
                    assert json.loads(command(common+['-mode','health'],folder,'cli-health'))['serving']
                    assert len(json.loads(command(common+['-mode','methods'],folder,'cli-methods')))>=4
                    assert json.loads(command(common+['-method','/demo/json','-request','{"hello":"world"}'],folder,'cli-json'))=={'hello':'world'}
                    schema=['-descriptor',str(ROOT/'api/schema.binpb')]
                    assert json.loads(command(common+schema+['-method','/kanarpc.examples.EchoService/Echo','-request','{"text":"cli"}'],folder,'cli-protobuf'))['text']=='cli'
                    rows=command(common+schema+['-method','/kanarpc.examples.EchoService/Watch','-request','{"text":"cli","count":3}'],folder,'cli-stream').splitlines();assert len(rows)==3 and all(json.loads(row)['text']=='cli' for row in rows)
                    command(common+['-mode','health','-token-env','KANARPC_BAD_TOKEN'],folder,'cli-bad-credential',expect=1);assert 'unauthenticated' in (folder/'cli-bad-credential.stderr').read_text()
                    metrics=request(info['Admin']+'/metrics').decode();assert 'kanarpc_gzip_prepared_total' in metrics and env['KANARPC_DEMO_TOKEN'] not in metrics;(folder/'metrics.txt').write_text(metrics)
                    server.terminate();server.wait(timeout=12);assert server.returncode==0
                    logs=(folder/'server.stderr').read_text();assert env['KANARPC_DEMO_TOKEN'] not in logs and '"trace_id"' in logs
                    client_events=[json.loads(line) for line in (folder/'client.stderr').read_text().splitlines() if line.startswith('{') and '"trace_id"' in line]
                    server_events=[json.loads(line) for line in logs.splitlines() if line.startswith('{') and '"trace_id"' in line]
                    server_traces={row['trace_id'] for row in server_events};assert len(client_events)>=5 and all(row['trace_id'] in server_traces for row in client_events)
                    (folder/'trace-correlation.json').write_text(json.dumps({'client_spans':len(client_events),'matched_trace_ids':[row['trace_id'] for row in client_events]},indent=2)+'\n')
                    report=json.loads((folder/'server.json').read_text());assert report['drain']['Quiescent'] and not any(report['drain']['TotalFaults'].values()) and report['captured_spans']>=9
                    key=(namespace+'/').encode();end=key[:-1]+bytes([key[-1]+1]);remaining=json.loads(request(endpoint+'/v3/kv/range',{'key':base64.b64encode(key).decode(),'range_end':base64.b64encode(end).decode()}));assert int(remaining.get('count',0))==0
                    (folder/'withdrawal.json').write_text(json.dumps(remaining,indent=2)+'\n');print(transport,'PASS',flush=True)
                finally:
                    if server.poll() is None:
                        server.terminate()
                        try:server.wait(timeout=8)
                        except subprocess.TimeoutExpired:server.kill();server.wait(timeout=3)
                    serverlog.close();(folder/'ownership.json').write_text(json.dumps({'owned_pid':server.pid,'reaped':server.poll() is not None})+'\n')
        finally:
            if process.poll() is None:process.terminate();process.wait(timeout=8)
            log.close();metadata['etcd']['reaped']=process.poll() is not None
        if before!=hashes():raise RuntimeError('source changed during acceptance')
        metadata['status']='PASS';metadata['binaries']={f.name:hashlib.sha256(f.read_bytes()).hexdigest() for f in [demo,cli]}
    except BaseException as e:metadata['status']='FAIL';metadata['failure']=repr(e);raise
    finally:(out/'result.json').write_text(json.dumps(metadata,indent=2)+'\n')
if __name__=='__main__':main()
