#!/usr/bin/env python3
"""Check archived receipts, every offered record, ownership and source hashes."""
from pathlib import Path
import gzip
import hashlib
import json
import tarfile

BASE = Path(__file__).resolve().parent


def read_json(t, name):
    return json.load(t.extractfile(name))


def sources(name):
    with tarfile.open(BASE / name) as t:
        return {m.name: hashlib.sha256(t.extractfile(m).read()).hexdigest()
                for m in t.getmembers() if m.isfile()}


def match(expected, actual):
    for name, digest in expected.items():
        assert actual.get(name) == digest, (name, 'source mismatch')


def main():
    final = sources('source.tar.gz')
    pre = sources('pre-hook-source.tar.gz')
    with tarfile.open(BASE.parent / 'grpc-r14/source.tar.gz') as t:
        baseline = {m.name: hashlib.sha256(t.extractfile(m).read()).hexdigest()
                    for m in t.getmembers() if m.isfile()}
    report = {'status': 'PASS', 'scope': 'independent archive integrity and receipt check; not a new RPC execution', 'checks': {}, 'comparisons': {}}
    with tarfile.open(BASE / 'checks.tar.gz') as t:
        for key in ['normal', 'race']:
            receipt = read_json(t, key + '/result.json')
            rows = [json.loads(line) for line in t.extractfile(key + '/test.jsonl')]
            counts = {action: sum(row.get('Action') == action and bool(row.get('Test')) for row in rows)
                      for action in ['pass', 'skip', 'fail']}
            assert counts == receipt['checks'] == {'pass': 421, 'skip': 0, 'fail': 0}
            assert receipt['status'] == 'PASS' and receipt['test_exit_code'] == 0 and receipt['owned_process_reaped']
            report['checks'][key] = counts
        for key in ['chain', 'chain-race']:
            receipt = read_json(t, key + '/result.json')
            assert receipt['status'] == 'PASS' and receipt['etcd']['reaped'] and len(receipt['runs']) == 23
            assert sum(row['exit_code'] != 0 for row in receipt['runs']) == 3  # rejected credentials
            match(receipt['source_sha256'], final)
            for transport in ['tcp', 'tls', 'quic']:
                prefix = key + '/' + transport + '/'
                client = read_json(t, prefix + 'client.json')
                server = read_json(t, prefix + 'server.json')
                assert client['passed'] and client['discovery_etcd'] and client['response_bytes'] == 65536
                assert client['stream_chunks'] == 20 and client['cancel'] == 'completed'
                assert client['trailers']['request-id'] == 'complete-chain'
                assert server['drain']['Quiescent'] and not any(server['drain']['TotalFaults'].values())
                assert read_json(t, prefix + 'ownership.json')['reaped']
                assert int(read_json(t, prefix + 'withdrawal.json').get('count', 0)) == 0
                logs = {}
                for side in ['client', 'server']:
                    logs[side] = [json.loads(line) for line in t.extractfile(prefix + side + '.stderr')
                                  if line.startswith(b'{') and b'"trace_id"' in line]
                server_ids = {row['trace_id'] for row in logs['server']}
                assert len(logs['client']) == 5 and all(row['trace_id'] in server_ids for row in logs['client'])
            report['checks'][key] = {'transports': 3, 'commands': 23, 'correlated_client_spans': 15}
        for key in ['sdk', 'module']:
            assert read_json(t, key + '/result.json')['status'] == 'PASS'
    for archive, runtime in [('default-final.tar.gz', final), ('default-pre-hook.tar.gz', pre)]:
        with tarfile.open(BASE / archive) as t:
            receipt = read_json(t, 'default/result.json')
            manifest = read_json(t, 'default/manifest.json')
            assert receipt['status'] == 'PASS' and len(receipt['runs']) == 72
            match(manifest['source_sha256'], runtime)
            match(manifest['baseline_source_sha256'], baseline)
            offered = 0
            for run in receipt['runs']:
                prefix = 'default/' + run['run'] + '/'
                data = json.loads(gzip.decompress(t.extractfile(prefix + 'client.json.gz').read()))
                offers = data['offers']
                assert len(offers) == data['offered'] == run['offered'] == data['succeeded']
                assert data['failed'] == run['failed'] == 0 and not any(row.get('Error') for row in offers)
                assert len({row['ID'] for row in offers}) == len(offers)
                assert read_json(t, prefix + 'server.json')['Active'] == 0
                assert read_json(t, prefix + 'ownership.json')['reaped']
                if data['engine'] == 'kana':
                    drain = read_json(t, prefix + 'server.json.drain.json')
                    assert drain['Quiescent'] and not any(drain['TotalFaults'].values())
                offered += len(offers)
            report['comparisons'][archive] = {'runs': 72, 'offered': offered, 'failed': 0}
    for line in (BASE / 'SHA256SUMS').read_text().splitlines():
        digest, name = line.split('  ', 1)
        assert hashlib.sha256((BASE / name).read_bytes()).hexdigest() == digest, name
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
