# R7C open-loop fixture controls

real serial loopback open-loop RPC, 4 clients, generator max 128 and lateness gate 50ms; dynamic-slot/timer fixture, no model or production-capacity claim

| Scene | Profile | Offered | OK | Fast OK/s | Fast p95 ms | On-time total | Pair CPU s |
|---|---|---:|---:|---:|---:|---:|---:|
| unattainable | fixed-fair | 440 | 440 | 19.6 | 1.499 | 374 | 0.251 |
| unattainable | adaptive | 440 | 296 | 19.3 | 1.541 | 135 | 0.215 |
| unattainable | feedback | 440 | 275 | 19.6 | 1.568 | 271 | 0.226 |
