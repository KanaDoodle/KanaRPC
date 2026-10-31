# R7A：独立上下文优先内核验收

2026-10-31，macOS / Apple M5 / arm64。此报告只覆盖新目录 `KanaRPC` 的第一阶段。原 KanaRPC-Go 的 QUIC、etcd、过载控制、模型适配和 CampusTrace 接入仍在旧目录；未将这些功能伪装为已在新版完成。

## 本轮交付

从新目录实现 `KNR2` 实验协议、显式泛型注册、普通/服务端流式调用的统一生命周期、信用流控、默认远端取消、相对执行预算、有界接纳、正常终态随机回执 ACK、截止时间 drain、资源统计，以及两个独立进程的可运行验收入口。没有复制旧 Go 源文件或通过包装调用旧 internal。

本轮没有更改旧项目源代码、其 GitHub fork 关系、远端仓库或 SDK；没有修改 CampusTrace 配置、依赖或数据库。旧项目的 294 项检查、QUIC 对照和模型 fixture 结果不能计为新版的测试。

来源核对见 [origins.json](origins.json) 与 [ORIGINS.md](../../../ORIGINS.md)。本地 upstream ref 为 `0e678f3e0704d1015e8db2c7e1a5b10491938038`，并非本轮联网确认的最新远端 HEAD。该 ref 有 28 个 Go 文件；旧工作区对应文件 24 个修改、4 个相同。新版核心 `./...` 的非标准库包只有自身模块；旧依赖只存在于明确分开的对照工具模块。整文件 hash 核对没有发现与旧 Go 文件完全相同的新版文件，但这不是法律或完整相似性证明，也不能称为严格 clean-room。

## 初稿实际复现并修复的问题

以下全部是**本轮新内核开发过程**发现的问题，不能写成修复了七个上游漏洞。早期普通/race 通过后，进一步的同步屏障与最小测试仍发现问题；原始失败证据保留。

| 问题 / 最小复现 | 根因 | 修改与回归 |
| --- | --- | --- |
| 未发送的 Open 被取消后污染连接；[`cancel-unsent-before.jsonl`](cancel-unsent-before.jsonl) 三轮失败 | 在 ID 已分配而记录未写出时，取消回调发送未来 ID 的 CANCEL，服务端正确判为协议错误 | 在 writer 许可内记录完整 Open 发送成功，取消取得同一许可后只为已发送请求写控制帧；对应 race 五轮通过 |
| 调用超时有时返回 `*net.OpError`；[`io-deadline-before.jsonl`](io-deadline-before.jsonl) | socket 定时器先于 Context 定时器被调度，丢失 `errors.Is(DeadlineExceeded)` 语义 | 区分 Context 提供的 IO 截止边界并归一化该边界的网络超时；race 五轮通过 |
| 处理器返回 `context.Canceled` 时丢失响应；[`error-boundary-before.jsonl`](error-boundary-before.jsonl) | 将处理器报告的取消状态等同于当前请求已被调用方取消 | 根据实际请求 Context 决定是否停止响应，处理器错误正常编码成状态；race 五轮通过 |
| typed-nil Status / 自定义错误 Is panic 越过错误边界；同上日志 | 只保护了处理器调用，错误分类在保护范围之外 | nil-safe Status，错误分类也设置 panic 边界；意外错误、panic 和敏感值不直接回传；race 五轮通过 |
| 编码耗时未计入配置预算；[`encoding-budget-before.jsonl`](encoding-budget-before.jsonl) | 在 json.Marshal 后才创建配置超时 | 在编码前开始预算，编码返回时已过期则不发送；不能抢占用户 MarshalJSON；race 五轮通过 |
| ACK 已收到，稍后的连接退休却记为丢失；[`ack-retirement-before.jsonl`](ack-retirement-before.jsonl) | END 写后 deadline-reset 错误或多个 select 同时就绪，覆盖已经成立的 ACK 观察 | 回执优先且不可被后续断连推翻，统一交付统计；屏障/自然断连/缺 ACK 用例 race 五轮通过 |
| 协议错误被次生 Open 写错误覆盖；[`min-race-before-open-arbitration.jsonl`](min-race-before-open-arbitration.jsonl) 与 [`open-arbitration-before.jsonl`](open-arbitration-before.jsonl) | 读循环先完成协议失败，写循环直接返回之后产生的 socket 错误，绕过已完成请求状态 | 保存连接首次失败原因，Open 收尾遵守请求已完成的终态；补充协议失败和远端拒绝两个同步屏障；race 五轮及最低版本全量回归通过 |

对应修复日志包括 [`targeted-regression.jsonl`](targeted-regression.jsonl)、[`error-boundary-after.jsonl`](error-boundary-after.jsonl)、[`encoding-budget-after.jsonl`](encoding-budget-after.jsonl)、[`ack-retirement-after.jsonl`](ack-retirement-after.jsonl)、[`open-arbitration-after.jsonl`](open-arbitration-after.jsonl)。未加屏障的 ACK/断连测试最初未失败，也保留为边界记录，未把推测当作已复现缺陷。

## 最终测试与实际验证

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| Go 1.27.1 普通测试 | 49 pass / 0 skip / 0 fail | [final-test.jsonl](final-test.jsonl) |
| Go 1.27.1 race | 49 pass / 0 skip / 0 fail | [final-race.jsonl](final-race.jsonl) |
| Go 1.26.0 普通测试 | 49 pass / 0 skip / 0 fail | [min-test.jsonl](min-test.jsonl) |
| Go 1.26.0 race | 49 pass / 0 skip / 0 fail | [min-race.jsonl](min-race.jsonl) |
| go vet | PASS | [vet.txt](vet.txt) |
| 帧 fuzz，15 秒、2 worker | 3,578,934 executions，PASS | [fuzz.txt](fuzz.txt) |
| 三种实现 × 取消/drain × 3 轮 | 18 轮行为对照完成 | [comparison.jsonl](comparison.jsonl) |
| 两个独立进程，真实 TCP + SIGTERM | PASS | [process-result.json](process-result.json) |

49 是包含子用例和 fuzz seed 的测试结果数，不是 49 种独立机制。有限 race/fuzz 不能证明没有任何 race、泄漏或安全漏洞。这里没有真实模型测试，因此“0 skip”不能理解成真实 vLLM 验收完成。

覆盖：并发普通/流式混合、顺序/空流/远端错误前的块保留、默认取消和执行期限、慢读流控、满载下控制消息、客户端容量、预取消/未发送取消、迟到响应与未知 ID、错误序号/credit/receipt/ID 复用、坏帧、处理器 panic/错误边界、编码预算、阻塞写、ACK 预算、关闭/发起请求竞争、drain 封闭接纳、终态 ACK、不可取消处理器的真实资源占用。

进程验收：一个服务端和一个客户端；普通 Echo 成功、20 个整数 chunk 顺序正确；用户关闭等待流后，服务端取消计数为 1；SIGTERM 时 `drain_handler_active=true` 且在途计数非零；原流继续交付第二个结果和 EOF；最终 Client.Pending / InFlightBufferedChunks 与 Server.Connections / InFlight / Executing / LostTerminals 均为零。进程均已回收，没有部署真实模型。

## 行为对照：保留已有能力和负结论

对照工具是独立嵌套模块，使用同一个 barrier 业务行为，分别调用公开 API。每轮单独进程、loopback、三次重复；旧默认与旧协作调用使用 Message envelope，新版用 typed int，因此这些结果只比较语义，不比较编码或性能。

| 实现 | 用户取消后、显式放行前远端退出 | drain 后原请求结果送达 |
| --- | --- | --- |
| 旧版默认普通调用 | 0/3 | 3/3 |
| 旧版 CooperativeUnary | 3/3 | 3/3 |
| 新版默认调用 | 3/3 | 3/3 |

取消观察窗口为 100 ms，旧默认处理器仍等待显式 release。旧默认 drain 在本次用例成功送达结果，不能声称它必然丢响应。新版没有击败旧协作模式：当前收益是把 Context/取消做成默认注册与协议契约，并独立实现可解释的资源边界。旧 Shutdown 的历史问题与此处 Drain 用例不同，不能混用结论。

## 本地开销基线

[`benchmark.txt`](benchmark.txt)：Apple M5、darwin/arm64、Go 1.27.1、GOMAXPROCS=2，串行 loopback 调用，连接在测量前建立。每项 1 秒 × 3 次，功能/race/fuzz 已结束后单独运行。

| 场景 | 测得范围 | 分配 |
| --- | --- | --- |
| 一次普通调用，含正常终态 ACK | 40.0–46.4 µs/op | 6.85–6.88 KB/op，103 allocs/op |
| 一次 16 块服务端流，含信用与终态 ACK | 131.5–144.1 µs/stream | 19.67–19.73 KB/stream，367–369 allocs/stream |

这是新版自身的开发基线，不是与旧框架、gRPC 或 QUIC 公平比较的收益，也不是单 chunk 耗时、生产容量或真实推理延迟。当前 JSON/回调/Context/状态通知分配较多，尚未做长时间 heap/GC 验证。

## 尚未解决的设计风险

- TCP 的写阻塞、坏帧和连接失败仍影响同一连接内多个请求；尚未实现新版原生 QUIC、安全 TLS 构造与身份验证。
- 拒绝响应和客户端 ACK 仍同步使用有期限的 TCP writer；慢 socket 下可能延迟该连接控制消息处理，没有独立控制通道或硬性即时取消保证。
- 数量与帧大小乘积仍可能较大，尚无跨流字节预算。已经完成的 Stream 缓冲由调用者持有，在途统计不能代表应用全部内存。
- 相对预算不依赖跨机同一绝对时间，但不扣除单向网络传输；控制消息无法送达时，没有客户端/服务端同一时刻停止的保证。
- 无法抢占用户处理器、MarshalJSON、自定义错误分类中的无限阻塞或恶意自定义 Conn/Listener 操作。Wait/统计如实保留未返回的处理器；自定义 IO 必须遵守 deadline 和 Close 契约。
- ACK 是协议观察，不是应用消费、持久化、exactly-once 或撤销业务副作用；LostTerminals 为服务端整个生命周期累计值。
- 当前读超时限制完整一帧及空闲间隔；没有自动重连、连接池、etcd 一致性、熔断、压力反馈或公平调度。注册冻结，没有热更新。
- 新版 CampusTrace 接入、可选模型后端、权限/重复调用/UNKNOWN 恢复尚未迁移；不能据旧 M4 结果宣称新版端到端完成。
- 没有真实 vLLM/GPU、跨机丢包、网络迁移、长期 soak 或生产流量证据。

## 下一步最值得做的三件事

1. **R7B 的请求交换与物理 session 分离**：TLS 信任配置、原生 QUIC 每请求一条流，再做故障隔离与 TCP/TLS 对照；etcd 保留快照/增量/失效的明确契约。
2. **在独立内核接回 R7C 治理**：先确定可信需求与成本口径，再做方法公平、反馈新鲜度和并发控制；保留固定/FIFO 与退化，不让 transport 统计冒充执行压力。
3. **资源与应用迁移验收**：补跨流字节预算和长期资源曲线，随后通过独立 SDK 候选迁移 CampusTrace；重新验证流式/取消/权限/恢复，真实模型单独验收。

具体阶段边界见 [rewrite-plan.md](../../rewrite-plan.md)。本轮只完成 R7A。
