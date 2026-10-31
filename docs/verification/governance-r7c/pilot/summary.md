# R7C open-loop fixture controls

real serial loopback open-loop RPC, 4 clients, generator max 128 and lateness gate 50ms; dynamic-slot/timer fixture, no model or production-capacity claim

| Scene | Profile | Offered | OK | Fast OK/s | Fast p95 ms | On-time total | Pair CPU s |
|---|---|---:|---:|---:|---:|---:|---:|
| capacity-shift | fixed-fifo | 1400 | 1193 | 164.4 | 58.368 | 993 | 0.649 |
| capacity-shift | fixed-fair | 1400 | 1226 | 190.3 | 5.053 | 1046 | 0.735 |
| capacity-shift | fixed4-fair | 1400 | 1044 | 188.1 | 1.575 | 407 | 0.783 |
| capacity-shift | adaptive | 1400 | 1223 | 199.2 | 1.574 | 1022 | 0.772 |
| capacity-shift | feedback | 1400 | 1109 | 199.3 | 1.577 | 1101 | 0.686 |
| unattainable | fixed-fifo | 220 | 220 | 19.2 | 1.623 | 200 | 0.197 |
| unattainable | fixed-fair | 220 | 220 | 19.2 | 1.579 | 196 | 0.196 |
| unattainable | fixed4-fair | 220 | 220 | 19.3 | 1.639 | 220 | 0.190 |
| unattainable | adaptive | 220 | 177 | 18.7 | 1.634 | 123 | 0.193 |
| unattainable | feedback | 220 | 158 | 19.6 | 1.619 | 156 | 0.159 |
