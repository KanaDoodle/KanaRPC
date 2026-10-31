# grpc-go matched-workload comparison

This nested module keeps grpc-go v1.85.0 out of the framework's runtime dependency changes. Both engines share the generated Protobuf v1.36.12 messages in `pb/bench.proto`, the same handler implementation and response validator. KanaRPC uses its public Protobuf registration and Call/Stream APIs; grpc-go uses official generated stubs.

Run from the repository root:

```sh
python3 scripts/compare-grpc.py --smoke --race --duration 0.2 --repeats 1 --output /tmp/grpc-race-new
python3 scripts/compare-grpc.py --duration 2 --repeats 4 --output /tmp/grpc-paired-new
python3 scripts/summarize-grpc.py /tmp/grpc-paired-new
```

The output directory must not exist. The runner builds once, checks source hashes before/after, serializes every case, alternates engine order and reaps owned servers. Race measurements are correctness checks and do not enter performance tables.

Messages are generated with protoc 36.2, protoc-gen-go v1.36.12 and protoc-gen-go-grpc v1.6.2. To regenerate, put those two plugins on PATH, enter this directory and run:

```sh
protoc -I . --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative pb/bench.proto
```

See [the comparison report](../../docs/grpc-comparison-r13.md) for controlled conditions, full results, protocol differences and measurement scope. The ordinary Go test/build commands in the root module do not include this nested tool.

## Profiling and internal optimization comparisons

```sh
# Diagnostic CPU/allocation/mutex/block profiles in each process.
python3 scripts/compare-grpc.py --focus --profile --duration 3 --repeats 1 --output /tmp/grpc-profile-new
# Three variants, serialized with rotated/reversed order: frozen Kana, current Kana, grpc-go.
python3 scripts/compare-grpc.py --baseline-root /absolute/frozen/source --long-stream --duration 2 --repeats 4 --output /tmp/grpc-optimization-new
python3 scripts/summarize-grpc.py /tmp/grpc-optimization-new
```

The frozen source must contain this nested module and its `replace ../..` target. Client/server binaries must use identical harness code and dependencies. `--long-stream` adds 64 chunks of 1024 bytes to exercise credit replenishment across multiple windows. `--focus` selects concurrency 16; it cannot be combined with `--smoke` (which selects concurrency 1). Profiled runs include sampling overhead and are diagnostic evidence only; compare performance with profiling disabled. Stop other tests and benchmarks before measurement, and keep both source trees unchanged until the run ends.

Each run retains all offered requests and checks the accepted connection count, server active calls, and Kana terminal receipts/drain faults. `PASS` means the runner completed its checks; read the explicit `failed` request counts as well. Source and binary hashes identify both variants; a source mutation makes the run fail.
