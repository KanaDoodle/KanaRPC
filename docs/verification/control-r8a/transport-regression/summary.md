# R7B serial transport controls

serial loopback closed-loop; 64 warmups excluded from request latency, included in whole-process CPU; no model, cross-machine or packet-loss claim

| Workload | Transport | Completed/offered | Mean req/s | Mean p95 ms | Mean pair CPU s |
|---|---|---:|---:|---:|---:|
| unary | tcp | 3000/3000 | 42468.1 | 0.299 | 0.0910 |
| unary | tls | 3000/3000 | 40066.0 | 0.321 | 0.0941 |
| unary | quic | 3000/3000 | 22944.7 | 0.532 | 0.1572 |
| stream | tcp | 3000/3000 | 7762.1 | 1.400 | 0.4577 |
| stream | tls | 3000/3000 | 8143.6 | 1.406 | 0.4375 |
| stream | quic | 3000/3000 | 9902.0 | 1.159 | 0.3538 |
