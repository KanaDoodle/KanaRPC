#!/usr/bin/env python3
"""One owned real HTTP/QUIC walkthrough. No benchmark/fuzz or inference claims."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request

ROOT=Path(__file__).resolve().parents[1]

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--binary',required=True,type=Path)
    parser.add_argument('--output',required=True,type=Path)
    args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False)
    ready=out/'ready.json';result={'scope':'same-process real loopback QUIC/HTTP; timer/slot fixture; no dual-host, WAN or inference validation','steps':[]}
    with (out/'server.log').open('wb') as log:
        process=subprocess.Popen([str(args.binary.resolve()),'-listen','127.0.0.1:0','-ready',str(ready)],cwd=ROOT,stdout=log,stderr=log)
        result['owned_pid']=process.pid
        def wait(predicate,timeout=8):
            until=time.monotonic()+timeout
            while time.monotonic()<until:
                if process.poll() is not None:raise RuntimeError('owned showcase exited')
                value=predicate()
                if value:return value
                time.sleep(.08)
            raise TimeoutError('walkthrough did not converge')
        try:
            wait(ready.exists)
            meta=json.loads(ready.read_text());url=meta['url'];result['url']=url
            def state():
                with urllib.request.urlopen(url+'/api/state',timeout=3) as response:return json.load(response)
            def post(path,body):
                req=urllib.request.Request(url+path,data=json.dumps(body).encode(),headers={'Content-Type':'application/json'})
                with urllib.request.urlopen(req,timeout=3) as response:return json.load(response)
            def save(name,value):
                (out/(name+'.json')).write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n');result['steps'].append(name)
            initial=state();assert initial['state']=='running';save('ready-state',initial)
            post('/api/preset',{'name':'overload'})
            # Observe actual work and a real controller decision, not a prescribed speedup.
            def overloaded():
                d=state()
                return d if d['stats']['Admitted']>=30 and d['governance']['Decisions'] else None
            loaded=wait(overloaded,10);save('overload',loaded)
            post('/api/preset',{'name':'stop'})
            wait(lambda: state()['active']==0,5)
            post('/api/stream',{'action':'start'})
            def streaming():
                d=state();return d if d.get('stream',{}).get('count',0)>=3 else None
            save('streaming',wait(streaming,5))
            post('/api/stream',{'action':'cancel'})
            def canceled():
                d=state()
                return d if d['stream']['state']=='canceled' and d['stats']['InFlight']==0 and d['client_memory']['Receive']['Used']==0 else None
            canceled=wait(canceled,5);assert canceled['stats']['Outcomes']['CancelRequests']>=1;save('canceled',canceled)
            post('/api/drain',{'omit_ack':False})
            drained=wait(lambda:(d if (d:=state())['state'] in ['drained','drain_failed'] and not d['busy'] else None),6)
            assert not drained.get('drain_error','') and drained['report']['Quiescent'] and drained['stats']['LostTerminals']==0,drained.get('drain_error','');save('normal-drain',drained)
            previous=drained['session'];post('/api/session',{'mode':'adaptive'})
            wait(lambda:(d if (d:=state())['session']>previous and d['state']=='running' and not d['busy'] else None),6)
            post('/api/drain',{'omit_ack':True})
            fault=wait(lambda:(d if (d:=state())['state']=='drain_failed' and not d['busy'] else None),6)
            assert 'terminal receipt not acknowledged' in fault['drain_error'] and fault['stats']['LostTerminals']>=1 and fault['report']['Quiescent'],fault['drain_error'];save('missing-ack-drain',fault)
            previous=fault['session'];lost=fault['total_lost'];post('/api/session',{'mode':'fixed'})
            fresh=wait(lambda:(d if (d:=state())['session']>previous and d['state']=='running' and not d['busy'] else None),6)
            assert fresh['stats']['LostTerminals']==0 and fresh['total_lost']>=lost and any(h['stats']['LostTerminals'] for h in fresh['history']);save('history-retained',fresh)
            result['status']='PASS'
        except BaseException as error:
            result['status']='FAIL';result['failure']=repr(error)
            try:save('failure-state',state())
            except Exception:pass
            raise
        finally:
            process.send_signal(signal.SIGTERM) if process.poll() is None else None
            try:code=process.wait(timeout=7)
            except subprocess.TimeoutExpired:process.kill();code=process.wait(timeout=3)
            result['exit_code']=code;result['owned_process_reaped']=process.poll() is not None
            result['race_reported']='DATA RACE' in (out/'server.log').read_text()
            if result.get('status')=='PASS' and (code or result['race_reported']):result['status']='FAIL';result['failure']='shutdown failure or race'
            (out/'result.json').write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps(result,ensure_ascii=False))
    if result['status']!='PASS':raise SystemExit(1)

if __name__=='__main__':main()
