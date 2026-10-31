# R7C open-loop fixture controls

real serial loopback open-loop RPC, 4 clients, generator max 128 and lateness gate 50ms; dynamic-slot/timer fixture, no model or production-capacity claim; race instrumentation: correctness only, exclude timings from performance conclusions

| Scene | Profile | Offered | OK | Fast OK/s | Fast p95 ms | On-time total | Pair CPU s |
|---|---|---:|---:|---:|---:|---:|---:|
| capacity-shift | feedback | 1400 | 1127 | 199.3 | 2.118 | 1121 | 0.938 |
