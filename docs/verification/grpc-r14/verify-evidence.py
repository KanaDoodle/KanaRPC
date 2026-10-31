#!/usr/bin/env python3
"""Validate preserved evidence; this does not rerun RPC workloads."""
import gzip
import hashlib
import json
from pathlib import Path
import tarfile

root = Path(__file__).resolve().parent
for line in (root / 'SHA256SUMS').read_text().splitlines():
    expected, name = line.split('  ', 1)
    assert hashlib.sha256((root / name).read_bytes()).hexdigest() == expected, name
result = json.loads((root / 'final-result.json').read_text())
assert result['status'] == 'PASS'
rows = {row['run']: row for row in result['runs']}
clients, servers, drains = set(), set(), set()
checked = 0
for archive in ['final-r0-r1.tar.gz', 'final-r2-r3.tar.gz']:
    with tarfile.open(root / archive, 'r:gz') as tar:
        for member in tar:
            path = Path(member.name)
            run = path.parent.name
            if path.name == 'client.json.gz':
                with gzip.GzipFile(fileobj=tar.extractfile(member)) as f:
                    data = json.load(f)
                assert run == data['run'] and run not in clients
                row = rows[run]
                assert len(data['offers']) == data['offered'] == row['offered']
                assert len({offer['ID'] for offer in data['offers']}) == data['offered']
                assert sum(bool(offer.get('Error')) for offer in data['offers']) == row['failed'] == 0
                checked += data['offered']
                clients.add(run)
            elif path.name == 'server.json':
                assert json.load(tar.extractfile(member))['Active'] == 0
                servers.add(run)
            elif path.name == 'server.json.drain.json':
                data = json.load(tar.extractfile(member))
                assert data['Quiescent'] and not any(data['TotalFaults'].values())
                drains.add(run)
assert clients == servers == set(rows)
assert drains == {name for name, row in rows.items() if row['engine'] == 'kana'}
assert checked == sum(row['offered'] for row in rows.values()) == 11702066
print(json.dumps({'status': 'PASS', 'runs': len(rows), 'checked_offers': checked, 'errors': 0}))
