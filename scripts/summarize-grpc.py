#!/usr/bin/env python3
"""Summarize every run; retain pairing, error counts and sample ranges."""
import argparse
import json
from pathlib import Path
import statistics


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument("directory",type=Path)
    args=parser.parse_args();directory=args.directory
    result=json.loads((directory/"result.json").read_text())
    if result["status"]!="PASS":raise RuntimeError("incomplete comparison: inspect original failures")
    groups={}
    for run in result["runs"]:
        k=(run['transport'],run['size'],run['workers'],run['chunks'])
        n=run['succeeded']
        if not n:raise RuntimeError("zero successful requests; inspect raw failure data")
        m={"rpc_s":n/(run['elapsed_ns']/1e9),"p50_us":run['p50_ns']/1e3,"p95_us":run['p95_ns']/1e3,"p99_us":run['p99_ns']/1e3}
        for role in ['server','client']:
            before,after=run[role+'_before'],run[role+'_after']
            m[role+'_cpu_us']=(after['CPU']-before['CPU'])/n/1e3
            m[role+'_B']=(after['Alloc']-before['Alloc'])/n
            m[role+'_allocs']=(after['Mallocs']-before['Mallocs'])/n
        rep=int(run['run'].split('_')[0][1:])
        groups.setdefault(k,{}).setdefault(run.get('variant',run['engine']),{})[rep]=m
    rows=[]
    for key,engines in sorted(groups.items()):
        if set(engines) not in [{'kana','grpc'},{'baseline','kana','grpc'}] or any(set(e)!=set(engines['kana']) for e in engines.values()):raise RuntimeError("unpaired results")
        row=dict(zip(['transport','size','workers','chunks'],key))
        row['metrics']={e:{k:statistics.median([v[k] for v in data.values()]) for k in next(iter(data.values()))} for e,data in engines.items()}
        row['samples']=engines
        ratios=[engines['kana'][r]['rpc_s']/engines['grpc'][r]['rpc_s'] for r in sorted(engines['kana'])]
        row['paired_ratio_median']=statistics.median(ratios);row['paired_ratio_range']=[min(ratios),max(ratios)]
        if 'baseline' in engines:
            ratios=[engines['kana'][r]['rpc_s']/engines['baseline'][r]['rpc_s'] for r in sorted(engines['kana'])]
            row['baseline_paired_ratio_median']=statistics.median(ratios)
            row['baseline_paired_ratio_range']=[min(ratios),max(ratios)]
        rows.append(row)
    summary={"runs":len(result['runs']),"offered":sum(r['offered'] for r in result['runs']),"failed":sum(r['failed'] for r in result['runs']),"rows":rows}
    (directory/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
    print(json.dumps({k:v for k,v in summary.items() if k!='rows'}))


if __name__=='__main__':main()
