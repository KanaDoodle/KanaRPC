# 验证记录索引

本目录按阶段保存源码快照、运行环境、报告与原始结果。阅读时先查看报告结论和对应源码版本，再使用同阶段的日志与 manifest 复现。产品能力与配置以 [README](../../README.md) 和当前技术文档为准。

## 阶段记录

| 阶段 | 主题 | 报告 |
| --- | --- | --- |
| R7A | v2 内核、取消、流控与关闭 | [内核](core-r7/report.md) |
| R7B | TLS/QUIC 与 etcd 注册发现 | [传输与发现](transport-r7b/report.md) |
| R7C | 方法调度、自适应与客户端反馈 | [过载治理](governance-r7c/report.md) |
| R8A | 固定限额回退与人工复位 | [控制回退](control-r8a/report.md) |
| R8B | 编码前接纳与周期资源观察 | [准备资源](resources-r8b/report.md) |
| R8C | 发现驱动的连接生命周期与 SDK | [连接管理](connections-r8c/report.md) |
| R8D | Darwin UDP 地址族冲突与网络故障 | [网络](network-r8d/report.md)、[握手根因](network-r8d/handshake-report.md) |
| R9A | 池级预算、退休统计与 QUIC reset 规避 | [共享预算](pool-r9a/report.md) |
| R9B | 有界原子批量与安全发送回退 | [QUIC 发送](quic-r9b/report.md) |
| R9C | 预算、取消、回执与累计故障 | [关闭契约](drain-r9c/report.md) |
| R9D | 小请求稳定性、profile 与候选取舍 | [内部基线](quic-r9d/report.md) |
| R10 | 交互控制台与端到端操作 | [控制台](showcase-r10/report.md) |
| R11 | Protobuf、类型化 SDK 与组件升级 | [编码与 SDK](protobuf-r11/report.md) |
| R12 | 能力协商、schema 与 SDK 版本化接入 | [SDK 治理](sdk-r12/report.md) |
| R13 | 普通/流式 TCP/TLS 与 grpc-go 同负载对照 | [外部基线](../grpc-comparison-r13.md) |
| R14 | pprof 定位、信用合并与 TCP writev | [并发与流式优化](../grpc-optimization-r14.md) |
| R15 | 业务接入、运维与完整调用链 | [接入与验收](production-r15/README.md) |

## 维护检查

[2026-10-31 文案审查](copy-review-20261031/report.md)：文档与界面措辞、链接、展示标识和 HTTP + QUIC 操作回归。

## 结果口径

历史报告、日志、压缩归档、截图和校验文件保留其生成时内容；阶段文档中的“后续”描述对应当时进度。当前组件关系与阶段状态见 [架构与演进](../rewrite-plan.md)。

性能对照记录完整 offered、成功、拒绝、超时与资源尾部；报告中的本地测量按当时环境解读。R9A 的 QUIC 流式吞吐退化约 52.9%，后续 R9B 采用有界批量降低发送成本；R9D 未稳定复现原小请求降幅，三个候选存在局部退化并全部撤回。原始失败与未采纳结果均保留。

验证环境包括本机 loopback、独立单成员 etcd 与本地 Linux VM 内核 netem。真实双机、WAN、多成员 etcd、小时/天级运行及模型推理性能尚未验证。完整使用与测量约束见 [使用约束](../operating-notes.md)。新验证记录应新增目录，沿用对应版本与运行收据，保持既有证据可核查。
