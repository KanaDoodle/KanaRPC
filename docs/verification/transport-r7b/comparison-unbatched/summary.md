# R7B serial transport controls

serial loopback closed-loop; 64 warmups excluded from request latency, included in whole-process CPU; no model, cross-machine or packet-loss claim

| Workload | Transport | Completed/offered | Mean req/s | Mean p95 ms | Mean pair CPU s |
|---|---|---:|---:|---:|---:|
| unary | tcp | 9000/9000 | 39692.7 | 0.320 | 0.2531 |
| unary | tls | 9000/9000 | 39838.0 | 0.295 | 0.2533 |
| unary | quic | 9000/9000 | 29253.6 | 0.402 | 0.3226 |
| stream | tcp | 9000/9000 | 8588.9 | 1.194 | 1.2029 |
| stream | tls | 9000/9000 | 9004.1 | 1.151 | 1.1511 |
| stream | quic | 9000/9000 | 13170.4 | 0.853 | 0.7203 |
