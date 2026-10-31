# R7C open-loop fixture controls

real serial loopback open-loop RPC, 4 clients, generator max 128 and lateness gate 50ms; dynamic-slot/timer fixture, no model or production-capacity claim

| Scene | Profile | Offered | OK | Fast OK/s | Fast p95 ms | On-time total | Pair CPU s |
|---|---|---:|---:|---:|---:|---:|---:|
| low-load | fixed-fifo | 120 | 120 | 10.2 | 1.621 | 120 | 0.095 |
| low-load | fixed-fair | 120 | 120 | 10.2 | 1.662 | 120 | 0.090 |
| low-load | fixed4-fair | 120 | 120 | 10.2 | 1.839 | 120 | 0.086 |
| low-load | adaptive | 120 | 120 | 10.2 | 1.727 | 120 | 0.089 |
| low-load | feedback | 120 | 120 | 10.2 | 1.625 | 120 | 0.086 |
| steady | fixed-fifo | 8400 | 8400 | 199.7 | 1.381 | 8400 | 0.969 |
| steady | fixed-fair | 8400 | 8400 | 199.7 | 1.371 | 8400 | 0.966 |
| steady | fixed4-fair | 8400 | 7119 | 194.1 | 1.494 | 1899 | 0.947 |
| steady | adaptive | 8400 | 8400 | 199.7 | 1.339 | 8400 | 0.884 |
| steady | feedback | 8400 | 8400 | 199.6 | 1.402 | 8400 | 1.012 |
| capacity-shift | fixed-fifo | 8400 | 6984 | 167.9 | 60.779 | 5797 | 0.956 |
| capacity-shift | fixed-fair | 8400 | 7151 | 191.2 | 4.659 | 6151 | 0.982 |
| capacity-shift | fixed4-fair | 8400 | 6209 | 194.1 | 1.415 | 1898 | 0.977 |
| capacity-shift | adaptive | 8400 | 7159 | 199.7 | 1.352 | 6116 | 0.794 |
| capacity-shift | feedback | 8400 | 6858 | 199.7 | 1.392 | 6846 | 0.794 |
| unattainable | fixed-fifo | 1320 | 1318 | 19.6 | 2.718 | 963 | 0.284 |
| unattainable | fixed-fair | 1320 | 1318 | 19.6 | 2.461 | 963 | 0.293 |
| unattainable | fixed4-fair | 1320 | 1316 | 19.6 | 1.549 | 857 | 0.279 |
| unattainable | adaptive | 1320 | 889 | 19.3 | 1.578 | 418 | 0.290 |
| unattainable | feedback | 1320 | 826 | 19.8 | 1.609 | 819 | 0.222 |
