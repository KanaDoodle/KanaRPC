#!/usr/bin/env python3
"""Losslessly archive owned experiment case directories after member validation."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil
import tarfile


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, type=Path)
    args = parser.parse_args()
    root = args.input.resolve()
    rows = json.loads((root/'results.json').read_text())
    # Only the case directories named by this runner's index are owned here.
    cases = [f"{r['seed']}-{r['payload_bytes']}-{r['workload']}-{r['transport']}-{r['strategy']}-{r['version']}" for r in rows]
    if len(set(cases)) != len(cases):
        raise ValueError('duplicate cases')
    if any(Path(name).name != name or (root/name).is_symlink() for name in cases):
        raise ValueError('case must be an owned direct directory')
    if not all(row.get('owned_processes_reaped') for row in rows):
        raise ValueError('owned processes not reaped; refusing removal')
    files = [file for name in cases for file in (root/name).rglob('*') if file.is_file()]
    if any(not (root/name/'result.json').is_file() for name in cases):
        raise ValueError('missing case; refusing removal')
    manifest = {str(file.relative_to(root)): sha(file) for file in files}
    archive = root/'raw-cases.tar.gz'
    if archive.exists():
        raise FileExistsError('never overwrite an existing archive')
    with tarfile.open(archive, 'w:gz') as out:
        for file in files:
            out.add(file, arcname=str(file.relative_to(root)), recursive=False)
    with tarfile.open(archive, 'r:gz') as source:
        members = {m.name: m for m in source.getmembers() if m.isfile()}
        if set(members) != set(manifest):
            raise ValueError('archive inventory mismatch')
        for name, digest in manifest.items():
            if hashlib.sha256(source.extractfile(members[name]).read()).hexdigest() != digest:
                raise ValueError('archive hash mismatch: '+name)
        offered = 0
        for name in cases:
            client = json.load(source.extractfile(members[name+'/client.json']))
            if len(client['offers']) != client['offered']:
                raise ValueError('missing offered records')
            offered += client['offered']
    receipt = dict(archive_sha256=sha(archive), members=manifest, cases=len(cases), offered=offered,
                   all_members_verified_before_removal=True, scope='all raw cases, including failed/degraded outcomes; root results remains a compact index')
    (root/'packaging-receipt.json').write_text(json.dumps(receipt, indent=2)+'\n')
    for name in cases:
        shutil.rmtree(root/name)
    print(json.dumps(dict(cases=len(cases), offered=offered, verified_members=len(manifest))))


if __name__ == '__main__':
    main()
