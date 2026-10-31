#!/usr/bin/env python3
"""Postprocess completed resource runs outside the measured processes."""
import argparse
import csv
import json
from pathlib import Path


def main():
    p=argparse.ArgumentParser();p.add_argument('--input',type=Path,required=True);p.add_argument('--output',type=Path,required=True);p.add_argument('--plot',action='store_true');args=p.parse_args()
    rows=json.loads((args.input/'summary.json').read_text());args.output.mkdir(parents=True,exist_ok=True);summary=[];curves=[]
    for row in rows:
        assert row['status']=='PASS'
        phases=row['client']['phases'];assert len(phases)==20
        clients=[v['client_heap'] for v in phases];servers=[v['server']['heap'] for v in phases]
        # Histories are streamed to disk; these checks run after exit. Exact
        # constant observed FD/goroutine counts are evidence for this window,
        # not thresholds claimed to prove absence of slow leaks forever.
        for values in [clients,servers]:
            assert all(v['FDs']>=0 for v in values)
            assert len({v['FDs'] for v in values})==1
            assert len({v['Goroutines'] for v in values})==1
        summary.append(dict(version=row['version'],transport=row['transport'],status='PASS',operations=row['client']['all_counts'],client_fd=clients[0]['FDs'],server_fd=servers[0]['FDs'],client_goroutines=clients[0]['Goroutines'],server_goroutines=servers[0]['Goroutines'],client_heap_min=min(v['HeapAlloc'] for v in clients),client_heap_max=max(v['HeapAlloc'] for v in clients),server_heap_min=min(v['HeapAlloc'] for v in servers),server_heap_max=max(v['HeapAlloc'] for v in servers)))
        for i,(a,b) in enumerate(zip(clients,servers)):
            curves.append(dict(version=row['version'],transport=row['transport'],cycle=i+1,nominal_active_seconds=(i+1)*30,client_heap_bytes=a['HeapAlloc'],server_heap_bytes=b['HeapAlloc'],client_fd=a['FDs'],server_fd=b['FDs'],client_goroutines=a['Goroutines'],server_goroutines=b['Goroutines']))
    (args.output/'resource-summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    with (args.output/'resource-curves.csv').open('w') as f:
        writer=csv.DictWriter(f,fieldnames=list(curves[0]));writer.writeheader();writer.writerows(curves)
    if args.plot:
        import matplotlib;matplotlib.use('Agg')
        import matplotlib.pyplot as plt
        fig,axes=plt.subplots(3,2,figsize=(11,9),sharex=True,layout='constrained')
        for index,transport in enumerate(['tcp','quic']):
            for version,color in [('baseline','#4263a8'),('current','#d66031')]:
                values=[v for v in curves if v['transport']==transport and v['version']==version];x=[v['nominal_active_seconds']/60 for v in values]
                axes[0,index].plot(x,[v['client_heap_bytes']/1048576 for v in values],marker='.',color=color,label=version)
                axes[1,index].plot(x,[v['client_fd'] for v in values],marker='.',color=color,label=version+' client')
                axes[1,index].plot(x,[v['server_fd'] for v in values],linestyle='--',color=color,label=version+' server')
                axes[2,index].plot(x,[v['client_goroutines'] for v in values],color=color,label=version+' client')
                axes[2,index].plot(x,[v['server_goroutines'] for v in values],linestyle='--',color=color,label=version+' server')
            axes[0,index].set_title(transport.upper());axes[0,index].set_ylabel('Client heap after GC (MiB)');axes[1,index].set_ylabel('Open FDs (observer included)');axes[2,index].set_ylabel('Goroutines');axes[2,index].set_xlabel('Nominal active time per case (minutes)')
            for axis in axes[:,index]:axis.grid(alpha=.2);axis.set_ylim(bottom=0);axis.legend(fontsize=8)
        fig.suptitle('R8C baseline vs R8D: 10-minute process-local resource observations\n8 callers, 20 load/cleanup cycles; not RSS or indefinite leak proof',fontsize=12)
        fig.savefig(args.output/'resources.png',dpi=160);fig.savefig(args.output/'resources.svg');plt.close(fig)
        (args.output/'plot-version.txt').write_text('matplotlib '+matplotlib.__version__+'\n')
    print(json.dumps(dict(status='PASS',cases=len(summary),cycles=len(curves))))


if __name__=='__main__':main()
