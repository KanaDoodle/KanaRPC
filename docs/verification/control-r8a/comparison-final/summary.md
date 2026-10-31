# RPC open-loop fixture controls

real serial loopback open-loop RPC, 4 clients, generator max 128 and lateness gate 50ms; dynamic-slot/timer fixture, no model or production-capacity claim

| Scene | Profile | Offered | OK | Fast OK/s | Fast p95 ms | On-time total | Pair CPU s |
|---|---|---:|---:|---:|---:|---:|---:|
| low-load | fixed-fifo | 120 | 120 | 10.2 | 1.831 | 120 | 0.105 |
| low-load | fixed-fair | 120 | 120 | 10.2 | 1.773 | 120 | 0.098 |
| low-load | fixed4-fair | 120 | 120 | 10.2 | 1.755 | 120 | 0.104 |
| low-load | fixed8-fair | 120 | 120 | 10.2 | 1.740 | 120 | 0.103 |
| low-load | adaptive | 120 | 120 | 10.2 | 1.731 | 120 | 0.090 |
| low-load | feedback | 120 | 120 | 10.2 | 1.744 | 120 | 0.105 |
| low-load | guarded | 120 | 120 | 10.2 | 1.722 | 120 | 0.102 |
| low-load | guarded-feedback | 120 | 120 | 10.2 | 1.727 | 120 | 0.105 |
| steady | fixed-fifo | 8400 | 8400 | 199.6 | 1.524 | 8400 | 1.355 |
| steady | fixed-fair | 8400 | 8400 | 199.6 | 1.511 | 8400 | 1.403 |
| steady | fixed4-fair | 8400 | 7091 | 193.9 | 1.558 | 1841 | 1.357 |
| steady | fixed8-fair | 8400 | 8400 | 199.7 | 1.498 | 8400 | 1.188 |
| steady | adaptive | 8400 | 8400 | 199.6 | 1.507 | 8400 | 1.116 |
| steady | feedback | 8400 | 8400 | 199.6 | 1.513 | 8400 | 1.423 |
| steady | guarded | 8400 | 8400 | 199.6 | 1.522 | 8400 | 1.336 |
| steady | guarded-feedback | 8400 | 8400 | 199.6 | 1.517 | 8400 | 1.422 |
| capacity-shift | fixed-fifo | 8400 | 6966 | 165.5 | 60.752 | 5793 | 1.362 |
| capacity-shift | fixed-fair | 8400 | 7154 | 193.0 | 4.411 | 6145 | 1.410 |
| capacity-shift | fixed4-fair | 8400 | 6145 | 194.1 | 1.528 | 1864 | 1.351 |
| capacity-shift | fixed8-fair | 8400 | 7176 | 199.6 | 1.521 | 6210 | 1.230 |
| capacity-shift | adaptive | 8400 | 7131 | 199.7 | 1.529 | 6067 | 1.255 |
| capacity-shift | feedback | 8400 | 6844 | 199.6 | 1.567 | 6830 | 1.289 |
| capacity-shift | guarded | 8400 | 7115 | 199.6 | 1.536 | 5996 | 1.420 |
| capacity-shift | guarded-feedback | 8400 | 6857 | 199.6 | 1.565 | 6847 | 1.248 |
| unattainable | fixed-fifo | 1320 | 1313 | 19.5 | 4.034 | 937 | 0.351 |
| unattainable | fixed-fair | 1320 | 1310 | 19.5 | 2.396 | 947 | 0.333 |
| unattainable | fixed4-fair | 1320 | 1314 | 19.5 | 1.650 | 706 | 0.366 |
| unattainable | fixed8-fair | 1320 | 1319 | 19.4 | 1.608 | 748 | 0.309 |
| unattainable | adaptive | 1320 | 885 | 19.3 | 1.622 | 408 | 0.290 |
| unattainable | feedback | 1320 | 818 | 19.9 | 1.635 | 810 | 0.227 |
| unattainable | guarded | 1320 | 1158 | 19.4 | 1.577 | 443 | 0.282 |
| unattainable | guarded-feedback | 1320 | 887 | 19.9 | 1.654 | 878 | 0.268 |
