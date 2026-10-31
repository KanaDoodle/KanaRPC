# R7B serial transport controls

serial loopback closed-loop; 64 warmups excluded from request latency, included in whole-process CPU; no model, cross-machine or packet-loss claim

| Workload | Transport | Completed/offered | Mean req/s | Mean p95 ms | Mean pair CPU s |
|---|---|---:|---:|---:|---:|
| unary | tcp | 3000/3000 | 45596.0 | 0.273 | 0.0871 |
| unary | tls | 3000/3000 | 44887.9 | 0.283 | 0.0868 |
| unary | quic | 3000/3000 | 26618.0 | 0.456 | 0.1337 |
| stream | tcp | 3000/3000 | 9472.5 | 1.126 | 0.3713 |
| stream | tls | 3000/3000 | 10137.4 | 1.094 | 0.3567 |
| stream | quic | 3000/3000 | 12343.5 | 0.938 | 0.2794 |
