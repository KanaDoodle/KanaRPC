#!/usr/bin/env python3
"""Describe paired local runs; retain failed pairs rather than selecting them out."""
import argparse
import json
from pathlib import Path
import random
import statistics


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True, type=Path)
    p.add_argument('--baseline', required=True)
    p.add_argument('--candidate', required=True)
    p.add_argument('--output', required=True, type=Path)
    a = p.parse_args()
    metadata = json.loads((a.input/'metadata.json').read_text())
    if metadata.get('profiled_diagnostic_only'):
        raise ValueError('profiled diagnostics are not throughput evidence')
    rows = json.loads((a.input/'results.json').read_text())
    groups = {}
    for row in rows:
        if row['version'] not in [a.baseline, a.candidate]:
            continue
        key = (row['transport'], row['workload'], row['strategy'], row['payload_bytes'])
        entries = groups.setdefault(key, {})
        identity = row['version'], row['seed']
        if identity in entries:
            raise ValueError('duplicate version/seed')
        entries[identity] = row
    summary = []
    for key, entries in sorted(groups.items()):
        seeds = sorted({seed for version, seed in entries})
        group = dict(zip(['transport', 'workload', 'strategy', 'payload_bytes'], key))
        group['pairs'] = len(seeds)
        group['failed_or_missing_pairs'] = [seed for seed in seeds if any((v, seed) not in entries or entries[v, seed]['status'] != 'PASS' for v in [a.baseline, a.candidate])]
        if group['failed_or_missing_pairs']:
            group['status'] = 'DEGRADED'
            summary.append(group)
            continue
        pairs = []
        for seed in seeds:
            old, new = entries[a.baseline, seed], entries[a.candidate, seed]
            rates = [r['client']['completed']*1e9/r['client']['elapsed_ns'] for r in [old, new]]
            pairs.append(dict(seed=seed, baseline_rps=rates[0], candidate_rps=rates[1], change_percent=(rates[1]/rates[0]-1)*100))
        changes = [r['change_percent'] for r in pairs]
        rng = random.Random(737)
        samples = sorted(statistics.median(rng.choices(changes, k=len(changes))) for _ in range(10000))
        median = lambda version, field: statistics.median(entries[version, s]['client'][field] for s in seeds)
        base = statistics.median(r['baseline_rps'] for r in pairs)
        candidate = statistics.median(r['candidate_rps'] for r in pairs)
        group.update(status='PASS', baseline_median_rps=base, candidate_median_rps=candidate,
                     ratio_of_medians_percent=(candidate/base-1)*100,
                     paired_median_change_percent=statistics.median(changes),
                     paired_change_range_percent=[min(changes), max(changes)],
                     paired_median_bootstrap_95_percent=[samples[249], samples[9749]],
                     negative_pairs=sum(c < 0 for c in changes), samples=pairs,
                     baseline_median_elapsed_ns=median(a.baseline, 'elapsed_ns'),
                     candidate_median_elapsed_ns=median(a.candidate, 'elapsed_ns'),
                     baseline_p95_ms=median(a.baseline, 'P95')/1e6,
                     candidate_p95_ms=median(a.candidate, 'P95')/1e6,
                     baseline_cpu_seconds=statistics.median(entries[a.baseline,s]['process_triple_cpu_seconds'] for s in seeds),
                     candidate_cpu_seconds=statistics.median(entries[a.candidate,s]['process_triple_cpu_seconds'] for s in seeds))
        summary.append(group)
    result = dict(baseline=a.baseline, candidate=a.candidate, input=str(a.input), groups=summary,
                  bootstrap_seed=737, bootstrap_resamples=10000,
                  scope='paired local descriptive bootstrap; shared-host drift and non-independent repetitions remain; not a production or causal confidence guarantee')
    a.output.write_text(json.dumps(result, indent=2)+'\n')
    for group in summary:
        print(group['transport'], group['workload'], group['status'], group.get('paired_median_change_percent'), group.get('paired_median_bootstrap_95_percent'), 'negative',group.get('negative_pairs'), '/',group['pairs'])


if __name__ == '__main__':
    main()
