#!/usr/bin/env python3
"""Owned Linux-VM kernel netem controls; not a real two-host experiment."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time

ROOT=Path(__file__).resolve().parents[1]
PROFILES={
    'baseline':[],
    'delay':['delay','10ms'],
    'loss':['loss','2%','seed','7'],
    'reorder':['delay','10ms','reorder','25%','50%','seed','7'],
    'rate':['rate','256kbit'],
    'combined':['delay','20ms','loss','1%','rate','512kbit','seed','7'],
}


def docker(*args,**kwargs):
    return subprocess.run(['docker',*args],check=True,text=True,capture_output=True,timeout=kwargs.pop('timeout',30),**kwargs).stdout


def case(binary_dir,output,image,version,transport,workload,profile,count,workers):
    output.mkdir();name=f'kanarpc-r8d-{os.getpid()}-{version}-{transport}-{workload}-{profile}'
    row=dict(version=version,transport=transport,workload=workload,profile=profile,planned_offered=count,actual_offered=0,netem=PROFILES[profile])
    owned=False
    try:
        init='set -e\n'
        if PROFILES[profile]:init+='tc qdisc add dev lo root netem limit 1000 '+' '.join(PROFILES[profile])+'\n'
        init+='tc -s qdisc show dev lo > /out/qdisc-start.txt\nuname -sr > /out/kernel.txt\nexec /lab/bin/'+version+' -mode worker -transport '+transport+' -ready /out/ready.json -output /out/worker.json\n'
        container=docker('run','--detach','--name',name,'--network','none','--cap-add','NET_ADMIN','--cpus','2','-e','GOMAXPROCS=2','--mount',f'type=bind,src={binary_dir},dst=/lab/bin,readonly','--mount',f'type=bind,src={output},dst=/out',image,'sh','-c',init).strip();owned=True;row['owned_container']=container
        until=time.monotonic()+15
        while not (output/'ready.json').exists():
            if time.monotonic()>until:raise TimeoutError('owned worker ready timed out')
            if docker('inspect','--format','{{.State.Running}}',name).strip()!='true':raise RuntimeError('owned worker startup failed')
            time.sleep(.02)
        with (output/'client.log').open('wb') as log:
            client=subprocess.run(['docker','exec',name,'env','GOMAXPROCS=2','/lab/bin/'+version,'-mode','client','-ready','/out/ready.json','-output','/out/client.json','-workload',workload,'-count',str(count),'-workers',str(workers),'-seed','7'],stdout=log,stderr=log,timeout=120)
        row['client_exit_code']=client.returncode
        if (output/'client.json').exists():
            row['client']=json.loads((output/'client.json').read_text());row['actual_offered']=len(row['client']['offers'])
        row['qdisc_after']=docker('exec',name,'tc','-s','qdisc','show','dev','lo');(output/'qdisc-after.txt').write_text(row['qdisc_after'])
        row['cgroup_cpu']=docker('exec',name,'cat','/sys/fs/cgroup/cpu.stat')
        docker('stop','--time','8',name,timeout=15)
        row['worker_exit_code']=int(docker('inspect','--format','{{.State.ExitCode}}',name).strip())
        if (output/'worker.json').exists():row['worker']=json.loads((output/'worker.json').read_text())
        if 'worker' not in row or 'client' not in row:raise RuntimeError('setup/warmup/output failure; measured offers may not have started')
        stats=row['worker']['stats'];assert all(stats[k]==0 for k in ['Connections','InFlight','Executing','NativeStreams'])
        assert row['worker']['duplicates']==0
        assert row['client']['completed']+64<=row['worker']['executions']<=count+64
        assert not any('corrupt' in o.get('error','') for o in row['client']['offers'])
        if client.returncode or row['worker_exit_code'] or row['client']['completed']!=count or stats['LostTerminals'] or stats['IncompleteExchanges']:
            row['status']='DEGRADED'
        else:row['status']='PASS'
    except Exception as error:row.update(status='FAIL',failure=repr(error))
    finally:
        if owned:
            try:(output/'worker.log').write_text(docker('logs',name))
            except Exception as error:row['log_error']=repr(error)
            try:docker('rm','--force',name);row['owned_container_reaped']=True
            except Exception as error:row.update(owned_container_reaped=False,reap_error=repr(error))
        (output/'result.json').write_text(json.dumps(row,indent=2)+'\n')
    return row


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--output',required=True,type=Path)
    parser.add_argument('--baseline-source',type=Path,required=True)
    parser.add_argument('--image',default='kanarpc-r8d-network:20261031')
    parser.add_argument('--profiles',nargs='+',choices=list(PROFILES),default=list(PROFILES))
    parser.add_argument('--transports',nargs='+',choices=['tcp','tls','quic'],default=['tcp','tls','quic'])
    parser.add_argument('--workloads',nargs='+',choices=['unary','stream'],default=['unary','stream'])
    parser.add_argument('--count',type=int,default=100)
    parser.add_argument('--workers',type=int,default=4)
    args=parser.parse_args();out=args.output.resolve();out.mkdir(parents=True,exist_ok=False);bins=out/'bin';bins.mkdir()
    metadata={'scope':'actual tc/netem on isolated container lo in local Docker Linux VM; two processes in one namespace, not physical dual host; timings under concurrent host resource observation are not standalone performance/capacity evidence','go':subprocess.check_output(['go','version'],text=True).strip(),'image':json.loads(docker('image','inspect',args.image))[0]['Id'],'profiles':args.profiles,'count':args.count,'workers':args.workers,'source_sha256':{},'binary_sha256':{}}
    for version,source in [('baseline',args.baseline_source.resolve()),('current',ROOT)]:
        subprocess.run(['go','build','-o',str(bins/version),'./cmd/transport-lab'],cwd=source,env=dict(os.environ,GOWORK='off',CGO_ENABLED='0',GOOS='linux',GOARCH='arm64'),check=True)
        metadata['source_sha256'][version]={str(p.relative_to(source)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(source.rglob('*.go')) if not any(v in p.relative_to(source).parts for v in ['docs','tools'])}
        for name in ['go.mod','go.sum']:metadata['source_sha256'][version][name]=hashlib.sha256((source/name).read_bytes()).hexdigest()
        metadata['binary_sha256'][version]=hashlib.sha256((bins/version).read_bytes()).hexdigest()
    metadata['driver_sha256']=hashlib.sha256(Path(__file__).read_bytes()).hexdigest();(out/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    for index,profile in enumerate(args.profiles):
        for workload in args.workloads:
            for transport in args.transports:
                for version in (['baseline','current'] if index%2==0 else ['current','baseline']):
                    r=case(bins,out/f'{profile}-{transport}-{workload}-{version}',args.image,version,transport,workload,profile,args.count,args.workers)
                    print(profile,transport,workload,version,r['status'],flush=True)
    rows=[]
    for p in sorted(out.glob('*/result.json')):
        r=json.loads(p.read_text())
        if 'client' in r:r['client']={k:v for k,v in r['client'].items() if k!='offers'}
        rows.append(r)
    (out/'summary.json').write_text(json.dumps(rows,indent=2)+'\n');print(json.dumps({'cases':len(rows),'actual_offered':sum(r['actual_offered'] for r in rows),'statuses':{s:sum(r['status']==s for r in rows) for s in ['PASS','DEGRADED','FAIL']}}))
    if any(r['status']=='FAIL' or not r.get('owned_container_reaped') for r in rows):raise SystemExit(1)


if __name__=='__main__':main()
