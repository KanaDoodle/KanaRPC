# R14 验证记录

完整报告：[并发与流式优化](../../grpc-optimization-r14.md)。

本轮先定位，再分别验证按需通知、信用合并和 TCP writev；最终为冻结原内核 / 优化内核 / grpc-go 的三方交替对照。原 R13 档案保持原样。

| 文件 | 内容 |
| --- | --- |
| `final-summary.json` | 192 组正式对照的配对中位数、范围、每轮值、CPU 与分配 |
| `final-result.json`、`final-manifest.json` | 全部组的聚合结果、依赖、参数、双版本源码/二进制 SHA-256 |
| `final-r0-r1.tar.gz`、`final-r2-r3.tar.gz` | `final/<run>/` 下的全部请求明细、ready、server、drain、stderr、PID 回收记录 |
| `candidates-lazy-credit.tar.gz` | `lazy/`、`credit/` 阶段全部候选结果与每次请求 |
| `candidate-writev.tar.gz` | `writev/` 阶段全部候选结果与每次请求；此阶段 baseline 为已合并信用的版本 |
| `diagnostics.tar.gz` | `profile-before/`、`profile-after/` 两端 CPU/allocs/mutex/block；`normal/`、`race/` 真实 etcd 测试；`compare-race/` 双进程 race；`quic/` 微基准；`logs/` 包含初次挂起的 SIGQUIT、编译失败及修正后结果 |
| `baseline-source.tar.gz` | 任何内核优化前的冻结源码，含相同 Go 对照工具 |
| `lazy-source.tar.gz`、`credit-source.tar.gz` | 候选 A、A+B 的源码，支持逐阶段重建 |
| `source.tar.gz` | 最终源码及报告；排除 verification 下的证据文件，保留其独立 go.mod |
| `*-result.json`、`*-summary.json` | 候选、race、真实 etcd 和 QUIC 的可直接读取结果 |
| `verification.json` | 原始请求逐条计数、唯一 ID、错误数与关闭结果的独立核查收据 |
| `SHA256SUMS` | 本目录各文件校验值 |
| `packaging.json` | 初次合并归档超过选定大小后拆包的记录，与测试/性能失败分开记录 |

所有单个归档小于 50 MiB。执行文件和 etcd data/WAL 不提交；profile 自带函数映射，可用 `go tool pprof -top <profile>` 阅读。源码归档用于重建及源码行定位。`profile-*` 与 `compare-race` 的吞吐均不进入正式性能表。

每份 `client.json.gz` 保存全部 offered 请求的延迟和错误。响应数量、正文和序号由实际 Go 客户端验证；归档核查脚本再次检查请求明细、计数、唯一 ID、server active 和 drain，而不将归档检查当作一次重新执行的 RPC 测试。

```sh
python3 docs/verification/grpc-r14/verify-evidence.py
```

采用完整四轮结果，包含 TCP 4096 B 单并发的下降样本、TLS 4096 B 并发 16 的下降/P95 结果、TLS 短流异常轮和候选未获吞吐收益的结果。真实双机、WAN、小时/天级运行尚未验证。
