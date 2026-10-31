# R7B：安全传输、原生 QUIC 与独立注册发现验收

2026-10-31。新模块 `github.com/KanaDoodle/KanaRPC`，Go 1.26.0 / 1.27.1，darwin/arm64 / Apple M5。已完成 [R7B 实施步骤](../../transport-r7b.md)；R7C 治理和 R7D SDK/CampusTrace 迁移仍未完成。

## 实现与来源

本轮新增 TLS 1.3、固定 ALPN/证书/可选 mTLS、原生 QUIC 每请求一个双向流、物理会话与交换资源分开统计、首帧和容量边界、终态 ACK/FIN/会话结束屏障、明确 UDP 所有权，以及独立 discovery 包。发现使用 v2 namespace、lease/内容所有权、线性一致快照 + revision watch、progress 新鲜度、断流/compaction 重建与显式传输筛选。

`recordChannel` 是一个记录通道，QUIC session 没有被包装成 net.Conn。每个原生流复用新版请求状态机，但只能接纳一次业务 OPEN；坏帧、取消、回执和信用均绑定该流。会话接纳与业务执行名额分别统计，不把原生流数量当成连接数或压力。

接入的是 [quic-go v0.64.1](https://github.com/quic-go/quic-go/releases/tag/v0.64.1) 与 etcd client/api v3.6.7；不是自行实现 QUIC、TLS、共识或新控制算法。实际验证服务端 etcd 是本机独立的 **3.7.1 单成员进程**。来源与依赖检查见 [origins.json](origins.json)，完整边界见 [ORIGINS.md](../../../ORIGINS.md)。旧 KanaRPC-Go 源码和 CampusTrace 配置/依赖/数据库没有修改；没有发布远端仓库或 SDK。

R7A 的原始源码与 hash 保留为 [r7a-source.tar.gz](r7a-source.tar.gz)、[r7a-source-manifest.json](r7a-source-manifest.json)，其历史 49 项验证仍只代表当时快照。

## 实际复现并修复的问题

这些是新版开发/回归发现的问题，不能写成修复了八个上游漏洞。原始失败、修复和阶段性通过均保留。

| 问题与复现 | 根因 | 修改与验证 |
| --- | --- | --- |
| 原生流只发一个字节仍超出首帧期限；[quic-initial.jsonl](quic-initial.jsonl) | 首帧 SetReadDeadline 被通用 reader 的 30 秒设置覆盖 | 将 first-record 限制纳入真正读取头/正文的 deadline，之后普通/race 回归 |
| 正常在途请求在 QUIC Drain 时偶发 unavailable；[quic-after.jsonl](quic-after.jsonl) 中重复用例 2/3 失败 | 收到终态 ACK 就释放名额，父会话关闭时服务端 FIN 仍可能未送达 | 服务端 FIN、客户端观察 EOF 后 FIN、服务端再释放正常交换名额；[quic-drain-after.jsonl](quic-drain-after.jsonl) 五轮通过 |
| 普通调用已成功，立即 Client.Close 却留下未完成交换；[client-close-before.jsonl](client-close-before.jsonl) 首轮失败 | 客户端 FIN 仍可能在发送队列；只等待本地 goroutine 退出不能证明服务端已退休交换 | 发布正常终态前封闭本流发送；Close 在独立 SESSION_BARRIER 控制流封闭父会话新接纳、确认旧接纳归零，再关闭父连接；[client-close-after.jsonl](client-close-after.jsonl)、[client-close-fixture-after.jsonl](client-close-fixture-after.jsonl) |
| 旧 lease 健康但 endpoint 已被别人替换，本地仍 Ready；[失败日志](staged/etcd-ownership-before/test.jsonl.gz) | 把 KeepAlive 成功等同于注册键仍属于自己 | 每次续租确认后线性一致读回 lease 和内容；恢复 CAS 同时比较 lease/内容；丢失所有权停止，撤销自身 lease 不删除新 owner；后续真实 etcd 回归通过 |
| 句柄退休后仍保留活 Context；[失败日志](staged/cleanup-before/test.jsonl.gz) | 所有权丢失的退出路径没有 cancel 子 Context | finish 总是取消自身生命周期；检查 done 后 Context 也结束，完整 race 通过 |
| 无法撤销 lease，Close 却返回 nil；[失败日志](staged/etcd-withdraw-before/test.jsonl.gz) | 清理 Revoke 错误被忽略 | Registration.Close 与 Registry.Close 返回未确认撤销错误；保留首次错误和累计数，避免无限错误历史；网络门关闭时实际键仍存在的最小用例通过 |
| 无效主机名、带加号端口、通配监听地址通过 endpoint 校验；[endpoint-before.jsonl](endpoint-before.jsonl) | 只做 SplitHostPort 和 Atoi | 校验主机段、数值端口、长度与非 unspecified 地址；[endpoint-after.jsonl](endpoint-after.jsonl) 与 endpoint fuzz 通过 |
| 截止时间已过，但取消尚未传播时仍触碰 socket，关闭共享连接；[早期完整 race](staged/final-race/test.jsonl.gz)、[确定性复现](deadline-propagation-before.jsonl) | R7A 只检查 ctx.Err，忽略 Deadline 可能先于 Done/Err 被观察到 | 在写许可前后、首次 IO 前同时检查 Deadline；无写入返回 DeadlineExceeded；原竞争与最小用例 race 各十轮通过：[deadline-propagation-after.jsonl](deadline-propagation-after.jsonl) |

测试隔离也做了调整：四个 Go 矩阵同时运行时，最低版本普通测试出现一次 3 秒 QUIC 握手超时，见 [早期日志](staged/min-test/test.jsonl.gz)。立即关闭测试先前将 40 个完整服务端 fixture 保留到整项测试结束；现在每次迭代立即关闭 listener 并等待资源回收，最终版本矩阵串行运行。之后完整矩阵和重复用例通过，但没有独立证明这次握手失败的唯一根因，不将其包装成已确认的框架漏洞。

## 最终检查

| 检查 | 结果 | 原始记录 |
| --- | --- | --- |
| Go 1.27.1 普通测试，独立真实 etcd | 98 pass / 0 skip / 0 fail | [verified-test/result.json](verified-test/result.json)、[test.jsonl](verified-test/test.jsonl) |
| Go 1.27.1 race，独立真实 etcd | 98 pass / 0 skip / 0 fail | [verified-race/result.json](verified-race/result.json)、[test.jsonl](verified-race/test.jsonl) |
| Go 1.26.0 普通测试，独立真实 etcd | 98 pass / 0 skip / 0 fail | [verified-min-test/result.json](verified-min-test/result.json) |
| Go 1.26.0 race，独立真实 etcd | 98 pass / 0 skip / 0 fail | [verified-min-race/result.json](verified-min-race/result.json) |
| go vet | PASS | [vet.txt](vet.txt) |
| 帧 fuzz，15s / 2 worker | 2,046,675 executions，PASS | [fuzz-frame.txt](fuzz-frame.txt) |
| endpoint fuzz，15s / 2 worker | 527,856 executions，PASS | [fuzz-endpoint.txt](fuzz-endpoint.txt) |
| 注册记录 fuzz，15s / 2 worker | 297,067 executions，PASS | [fuzz-record.txt](fuzz-record.txt) |
| R7A 双进程 SIGTERM 回归 | PASS | [result.json](r7a-process-regression/result.json) |
| R7A 新旧默认/协作语义回归 | 18 轮通过 | [comparison.jsonl](r7a-behavior-regression/comparison.jsonl) |
| 最终双进程负载 | 18/18，通过 54,000/54,000 次 offered | [summary.md](comparison-final/summary.md)、[summary.json](comparison-final/summary.json) |

98 包含子用例和 fuzz seed，不是 98 种独立机制。有限 race/fuzz 不证明不存在所有 race、泄漏、安全问题或容量退化。0 skip 也不代表模型验收完成：新版当前没有真实模型用例。

etcd runner 对每次独立实例先核对唯一 member name、client/peer URL 与进程状态，确认属于本任务后才允许 compaction。每轮创建独立目录和随机 loopback 端口，结果保存 membership 和 owned_process_reaped；全部自有进程已回收，不借用既有 etcd 或 Docker 服务。

etcd 覆盖：注册与撤销、不同传输筛选、冲突 ID、lease 被撤销后的恢复、健康 lease 下的键替换、CAS 所有权、未确认撤销错误、退休 Context、Get/Watch 间隙、同一事务同 revision 的两键、实际已压缩 revision 的 Watch 错误与重建、TCP 断流门下 readiness 到期和恢复最新状态、Created/历史事件不能刷新 freshness、元数据/快照容量，以及从发现结果通过 TCP/TLS/QUIC 实际调用。

传输覆盖：类型化并发与流、证书/主机名/mTLS、ALPN、慢握手、物理 session 和每 session 执行容量、客户端名额、原生流 slot 等待取消、首帧、坏帧、单流取消、每流一次 OPEN、终态回执、缺 ACK、FIN、立即关闭、deadline drain、实际 UDP 地址重绑。

## 故障对照

[fault-controls.jsonl](fault-controls.jsonl)：三种传输各重复三次，在同一物理连接/session 的邻近请求执行期间注入一个应用协议坏帧。

| 传输 | 邻近请求完成 | 同连接/session 后续请求完成 | 重新建连接恢复 | Hold 业务执行 |
| --- | --- | --- | --- | --- |
| TCP | 0/3 | 0/3 | 3/3 | 每轮一次，无重放 |
| TLS | 0/3 | 0/3 | 3/3 | 每轮一次，无重放 |
| QUIC | 3/3 | 3/3 | 无需重新连接 | 每轮一次，无重放 |

这验证原生流的应用帧故障隔离，不是 TCP 段丢失/UDP 丢包实验，也不意味着 QUIC 不共享拥塞、连接窗口、CPU 或父会话失败。TCP/TLS 可以通过新连接恢复，不能描述为故障后框架永久不可用。

## 最终负载对照与退化

同一个最终源码和二进制，两个独立进程，GOMAXPROCS=2，8 个封闭并发 worker，每轮 3,000 次请求，64 次连接 warmup，种子 7/19/41，轮转传输顺序。每个业务块包含 128 字节固定 payload；流场景每次 16 块。普通和流式调用都包含协议终态流程，QUIC 额外包含每请求原生流及 FIN 握手。真实网络是本机 loopback，业务数据是 fixture。

所有功能/race/fuzz 结束后才串行测量最终轮次。每种传输/负载累计 9,000 次 offered，全部成功；另有每轮 64 次 warmup，不计入请求分位数。全进程 CPU 包含初始化、warmup、生成负载和关闭，不能冒充 RPC-only ns/op。p95 是三轮成功请求 p95 的算术平均，原始 offered 与失败记录均保留。

| 负载 | TCP req/s | TLS req/s | QUIC req/s | TLS / QUIC p95 | TLS / QUIC 进程对 CPU |
| --- | ---: | ---: | ---: | --- | --- |
| 普通调用 | 51,616.2 | 51,120.3 | 29,602.8 | 0.250 / 0.391 ms | 0.1951 / 0.3177 s |
| 16 块流 | 10,418.1 | 11,229.9 | 13,800.3 | 0.947 / 0.802 ms | 0.9116 / 0.6881 s |

相对同版本 TLS：普通调用 QUIC 吞吐 **-42.1%**、p95 **+56.0%**、CPU **+62.8%**；16 块流吞吐 **+22.9%**、p95 **-15.4%**、CPU **-24.5%**。保留前者退化。后者仅是在这个短、低延迟、固定 payload/并发、封闭负载下的实测，不是跨机、丢包、开放到达、真实推理或生产扩展性结论。三次重复没有置信区间，不能据此保证普遍收益。

前一轮 [未合并写入对照](comparison-unbatched/summary.md) 的 QUIC 流吞吐优势更大；检查发现通用 writer 将头和正文分别写入，使 TLS 处理两次记录写入，而 QUIC 自带缓冲。最终所有传输统一合并一帧再写，补部分写入测试并完整回归后重新测量。早期对照与 [pilot](pilot/summary.md) 保留。期间也有关闭/deadline 修复，不能把前后全部差异因果归给写合并；最终跨传输对照使用同一份源码。

每轮 worker 重复业务 ID 为零；服务端执行数恰为 offered+64；最终连接、在途、执行、原生 reader、丢失终态和未完成交换计数为零。QUIC Opened=Retired=3,064、Active=0、CloseConfirmed=true。全部 raw offers、worker/client log 与公用实验 CA 在 [cases.tar.gz](comparison-final/cases.tar.gz)，source/binary hash 在 [metadata.json](comparison-final/metadata.json)。没有保存实验私钥。

## 尚未解决的设计风险

- **字节预算和长期资源验证**：连接/流/帧上限的乘积仍可能很大。默认 64 session × 64 stream × 1 MiB 的 payload 上限乘积可达 4 GiB，仅是条件计算，不是实际 RSS 测量；执行名额不限制未接纳流读取头后预分配的 payload。尚无全局跨流字节预算，也未做长时间 goroutine/heap/GC 曲线。写合并还增加最多一帧的临时复制。
- **传输取舍**：QUIC 的普通调用退化明显，共享父会话失败、拥塞和窗口；没有跨机丢包、重排、迁移、带宽受限或真实网络 RTT 验证。没有 HTTP/3、WebTransport、0-RTT 或多路径。
- **身份与权限**：mTLS 校验证书，不提供服务方法 ACL、租户身份和成本归属。QUIC session 上限是握手接收后的限制，不是完整抗 DoS。
- **发现与连接生命周期**：Ready 是有期限观察，不是即时健康或线性一致的每次路由决定；选择 snapshot 后到发送前仍可能变化。没有发现驱动连接池、地址更新回收、业务自动重试，撤销与 Drain 由应用显式协调。
- **控制面开销与故障覆盖**：每次续租做额外所有权 Get；每 resolver 请求 progress 会对共享 watcher 产生重复控制开销。固定短重连 backoff 未加随机退避或订阅合并；只验证单成员真实 etcd，没有多节点选主/仲裁丢失/跨集群恢复矩阵。
- **结束语义**：ACK/FIN/barrier 是协议观察，不是应用消费、数据库提交或 exactly-once；CloseConfirmed=false 仍会回收会话。忽略 Context 的处理器、编码器、自定义 IO 无法强制抢占；统计/Wait 如实反映未退出的业务处理器。
- **应用与模型**：新版还没有 R7C 熔断/自适应并发/公平调度，也未迁移推理 worker/gateway、CampusTrace SDK 和权限/去重/UNKNOWN 恢复。旧 M1–M4 证据不能用于宣称新版完成。真实 vLLM/GPU 和真实模型 API 推理收益均 **NOT_VALIDATED**。

## 下一步最值得做的三个地方

1. 在 R7C 前先补跨流字节预算和长期资源曲线，让反馈反映真实接纳/执行/排队，避免原生流数冒充负载。
2. 按 R7C 迁移服务端反馈、方法公平与自适应并发，用固定并发/FIFO 强基线，保留低负载和震荡退化；身份明确后再做租户成本公平。
3. 经独立 SDK 候选进入 R7D，迁移 CampusTrace 并重新验证流式、用户取消、权限、重复调用和现有任务恢复；跨机与真实模型单独验收。
