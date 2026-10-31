# R7B serial transport controls

serial loopback closed-loop; 64 warmups excluded from request latency, included in whole-process CPU; no model, cross-machine or packet-loss claim

| Workload | Transport | Completed/offered | Mean req/s | Mean p95 ms | Mean pair CPU s |
|---|---|---:|---:|---:|---:|
| unary | tcp | 3000/3000 | 45143.5 | 0.278 | 0.0937 |
| unary | tls | 3000/3000 | 46109.3 | 0.263 | 0.0830 |
| unary | quic | 3000/3000 | 27695.1 | 0.432 | 0.1316 |
| stream | tcp | 3000/3000 | 9676.3 | 1.105 | 0.3652 |
| stream | tls | 3000/3000 | 10093.8 | 1.107 | 0.3557 |
| stream | quic | 3000/3000 | 12799.6 | 0.896 | 0.2694 |
