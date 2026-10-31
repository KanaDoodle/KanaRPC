"""Continue only missing cases with the original frozen binaries; never retry."""
import hashlib, importlib.util, json
from pathlib import Path
root=Path.cwd();output=Path('/private/tmp/kanarpc-r8c-20261031/serialized');metadata=json.loads((output/'metadata.json').read_text())
for name,digest in metadata['source_sha256'].items():
 if name!='scripts/verify-routing.py':assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
for name,digest in metadata['binary_sha256'].items():assert hashlib.sha256((output/name).read_bytes()).hexdigest()==digest,name
spec=importlib.util.spec_from_file_location('routing',root/'scripts/verify-routing.py');m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
receipt={'scope':'same frozen binaries/source except controller failure aggregation; preserve every previously started case; no reattempt of failed warmup','driver_sha256':hashlib.sha256((root/'scripts/verify-routing.py').read_bytes()).hexdigest(),'started':[],'skipped_existing':[]}
for repeat,seed in enumerate([7,19,41]):
 for workload in ['unary','stream']:
  for transport in ['tcp','quic']:
   strategies=['direct','dial','managed','churn'];order=strategies[repeat:]+strategies[:repeat]
   for strategy in order:
    name=f'{seed}-{workload}-{transport}-{strategy}';directory=output/name
    if directory.exists():receipt['skipped_existing'].append(name);continue
    receipt['started'].append(name)
    try:r=m.case(output/'transport-lab',output/'routing-lab',directory,transport,workload,strategy,seed,1000,8);print(name,r['status'],flush=True)
    except Exception as error:print(name,'FAIL',repr(error),flush=True)
rows=[]
for path in sorted(output.glob('*/result.json')):
 row=json.loads(path.read_text())
 if 'client' in row:row['client']={k:v for k,v in row['client'].items() if k!='offers'}
 rows.append(row)
(output/'summary-complete.json').write_text(json.dumps(rows,indent=2)+'\n');(output/'continuation.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'cases':len(rows),'statuses':{s:sum(r['status']==s for r in rows) for s in ['PASS','DEGRADED','FAIL']},'actual_offered':sum(r.get('client',{}).get('offered',0) for r in rows)}))
