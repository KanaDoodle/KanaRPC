# R7C open-loop fixture controls

real serial loopback open-loop RPC, 4 clients, generator max 128 and lateness gate 50ms; dynamic-slot/timer fixture, no model or production-capacity claim; race instrumentation: correctness only, exclude timings from performance conclusions

| Scene | Profile | Offered | OK | Fast OK/s | Fast p95 ms | On-time total | Pair CPU s |
|---|---|---:|---:|---:|---:|---:|---:|
| capacity-shift | feedback | 1400 | 1126 | 199.2 | 1.881 | 1120 | 1.035 |
