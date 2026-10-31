# R7B serial transport controls

serial loopback closed-loop; 64 warmups excluded from request latency, included in whole-process CPU; no model, cross-machine or packet-loss claim

| Workload | Transport | Completed/offered | Mean req/s | Mean p95 ms | Mean pair CPU s |
|---|---|---:|---:|---:|---:|
| unary | tcp | 9000/9000 | 51616.2 | 0.241 | 0.1987 |
| unary | tls | 9000/9000 | 51120.3 | 0.250 | 0.1951 |
| unary | quic | 9000/9000 | 29602.8 | 0.391 | 0.3177 |
| stream | tcp | 9000/9000 | 10418.1 | 1.010 | 0.9723 |
| stream | tls | 9000/9000 | 11229.9 | 0.947 | 0.9116 |
| stream | quic | 9000/9000 | 13800.3 | 0.802 | 0.6881 |
