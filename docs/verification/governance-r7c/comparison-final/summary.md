# R7C open-loop fixture controls

real serial loopback open-loop RPC, 4 clients, generator max 128 and lateness gate 50ms; dynamic-slot/timer fixture, no model or production-capacity claim

| Scene | Profile | Offered | OK | Fast OK/s | Fast p95 ms | On-time total | Pair CPU s |
|---|---|---:|---:|---:|---:|---:|---:|
| low-load | fixed-fifo | 120 | 120 | 10.2 | 1.588 | 120 | 0.085 |
| low-load | fixed-fair | 120 | 120 | 10.2 | 1.581 | 120 | 0.081 |
| low-load | fixed4-fair | 120 | 120 | 10.2 | 1.585 | 120 | 0.086 |
| low-load | adaptive | 120 | 120 | 10.2 | 1.587 | 120 | 0.080 |
| low-load | feedback | 120 | 120 | 10.2 | 1.520 | 120 | 0.083 |
| steady | fixed-fifo | 8400 | 8400 | 199.7 | 1.314 | 8400 | 0.800 |
| steady | fixed-fair | 8400 | 8400 | 199.7 | 1.272 | 8400 | 0.855 |
| steady | fixed4-fair | 8400 | 7172 | 194.0 | 1.357 | 1879 | 0.792 |
| steady | adaptive | 8400 | 8400 | 199.7 | 1.262 | 8400 | 0.717 |
| steady | feedback | 8400 | 8400 | 199.7 | 1.243 | 8400 | 0.760 |
| capacity-shift | fixed-fifo | 8400 | 6974 | 165.2 | 60.547 | 5804 | 0.841 |
| capacity-shift | fixed-fair | 8400 | 7156 | 191.7 | 4.420 | 6143 | 0.852 |
| capacity-shift | fixed4-fair | 8400 | 6220 | 194.2 | 1.309 | 1861 | 0.768 |
| capacity-shift | adaptive | 8400 | 7157 | 199.7 | 1.286 | 6070 | 0.803 |
| capacity-shift | feedback | 8400 | 6897 | 199.7 | 1.301 | 6884 | 0.739 |
| unattainable | fixed-fifo | 1320 | 1318 | 19.6 | 1.943 | 985 | 0.244 |
| unattainable | fixed-fair | 1320 | 1316 | 19.6 | 1.489 | 985 | 0.244 |
| unattainable | fixed4-fair | 1320 | 1317 | 19.6 | 1.469 | 850 | 0.241 |
| unattainable | adaptive | 1320 | 889 | 19.4 | 1.448 | 421 | 0.242 |
| unattainable | feedback | 1320 | 828 | 19.8 | 1.476 | 822 | 0.196 |
