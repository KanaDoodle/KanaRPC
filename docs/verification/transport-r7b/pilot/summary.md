# R7B serial transport controls

serial loopback closed-loop; 64 warmups excluded from request latency, included in whole-process CPU; no model, cross-machine or packet-loss claim

| Workload | Transport | Completed/offered | Mean req/s | Mean p95 ms | Mean pair CPU s |
|---|---|---:|---:|---:|---:|
| unary | tcp | 80/80 | 24764.6 | 0.392 | 0.0409 |
| unary | tls | 80/80 | 21430.2 | 0.428 | 0.0268 |
| unary | quic | 80/80 | 26262.5 | 0.365 | 0.0359 |
| stream | tcp | 80/80 | 4778.7 | 2.128 | 0.0929 |
| stream | tls | 80/80 | 5844.4 | 2.188 | 0.0918 |
| stream | quic | 80/80 | 13237.8 | 0.826 | 0.0531 |
