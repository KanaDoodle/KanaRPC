# R9A 池级共享字节预算与退休对象统计

2026-10-31，从 R8D `90af591` 开始，仅修改新版 KanaRPC。CampusTrace、旧 KanaRPC-Go、依赖版本与 v2 wire 格式不改。实施步骤先记录于 [pool-r9a.md](../../pool-r9a.md)。本轮补齐的是跨根连接的接纳与可观测性；旧版遵守各连接限额，但没有用户要求的池级总额，不能把这项能力缺口写成旧版违反已有契约。

## 复现、根因与修改

| 实际问题/缺口 | 复现 | 根因与结果 |
| --- | --- | --- |
| 编码上限随连接数放大 | 两 endpoint，同步阻塞 MarshalJSON，每条连接 EncodingBytes=2048，拟设池上限 2048 | R8D 同时进入两个编码器，实际 4096；新版只有一个进入，另一个在有界调用者中等待，取消后不发送业务 |
| 接收/已完成未读结果可跨连接累积 | 两 endpoint，各完成 32 条 19-byte 整数 DATA，每连接 receive=1024，拟设池 receive=1024 | R8D 实际 1216；新版保持池峰值≤1024，第二连接/交换得到接收失败，第一流仍完整可读 |
| 退休对象不能解释持有字节 | 完成三条 `10/11/12` 后不读，更新发现到新 endpoint；再阻塞一条用户解码并 Close | 新版退休组六字节、三块；取出解码后未读降至两块而 Receive 仍六字节；Close 清掉另两块后，已关闭退休对象仍计费两字节，直到解码返回才释放 |
| 发送/接收分层接纳需要保证原子性 | 两个 net.Pipe，一方阻塞 700-byte DATA 发送（含 header 724），另一方再发送；接收方只发送 header 不提供 payload | 共享 class 拒绝第二个请求，不留下局部半租约；发送拒绝 touched=false；接收在分配 payload 前拒绝；取消/部分读取失败后归零 |
| 零字节控制租约与关闭账户不能被提前回收 | 持有零字节接收租约，Retire+OwnerClosed，32 个并发重复 release/shrink/snapshot | 账户在有效零字节租约存在时拒绝 Release，最终只释放一次；账户不能绑定两次、释放后不能重绑，迟到 Dial 仍被跟踪到实际 Close |

同一份兼容复现文件先在基线运行再在新版运行：[旧版两项失败](compatibility-before.jsonl)、[首次编码失败](encoding-before.jsonl)、[编码修复后](encoding-after.jsonl)、[分层租约与原有回归](focused-round2.jsonl)、[退休/饱和 5 轮](pool-stage3.jsonl)。反射只用于兼容复现与实验驱动，生产路由/编码无反射适配。

实现由以下边界构成：

- 根包 `MemoryPool/MemoryAccount` 以一把 mutex 保护池与连接局部预算，先检查全部约束再取得租约，缩减/释放同步更新。Encoding+Receive+Send 共用 Data 总额；控制收发各有独立保留额度。默认池 32 MiB 数据、16 MiB 分类、64 KiB 单向控制，数据总额不随 MaxConnections 放大。
- 每个 routing entry 在 Dial 前取得独立 account，dialing/active/retiring/closing/closed-with-users 均计入上限。Retire 只转移统计；Close 完成 IO 后仍有用户 codec/未读引用则继续保留。最终 Release 要求根 owner 已关闭、租约/等待者/未读块归零。不存在无限累积的退休历史列表。
- `Unread` 记录流/块/字节，为 Receive 的子集；取出数据后的用户解码仍由 receive lease 计费。根连接各自持有 bufferOwner，关闭一个连接不会清空其他连接。
- `MemoryStats()` 是原子池快照，`ConnectionStats()` 是有界对象明细，包含 endpoint、唯一账户 ID、退休时长、状态、引用及账户字节。pool 不回调 routing、pending 或 bufferOwner，避免倒置锁顺序。独立 SDK 示例已展示配置与最终账户回收。

## 正确性与故障验证

Go 1.27.1 / 最低 1.26.0，普通与 `-race` 四个最终完整运行，各 **266 PASS / 0 SKIP / 0 FAIL**。每次 runner 创建并核验自己的 etcd 3.7.1 单成员实例，退出后只回收自己启动的进程：[本机普通](recovery-final-go-local/result.json)、[本机 race](recovery-final-go-race/result.json)、[最低版本普通](recovery-final-go-minimum/result.json)、[最低版本 race](recovery-final-go-minimum-race/result.json)。不复用 R8D 测试数。

其他验证：池聚焦 race 30 轮 **480 PASS**；三传输饱和/退休等后补测试 5 轮 **105 PASS**；QUIC 反向黑洞下取消、退休和 Close 的 race 20 轮通过，日志可见实际丢包及最终零账户：[黑洞记录](pool-blackhole-race.jsonl)。本地应用 UDP relay 不是内核 netem 或真实双机。

`FuzzSharedByteLeases` 实际运行约 31 秒，179,971 次执行，43 个新 interesting input，通过；后半程执行数停在 179,971，保留原始日志，不据此宣称稳定 fuzz 吞吐：[fuzz](fuzz-pool.log)。独立 SDK 模块用公开 API、真实 TCP、race 三轮通过：[SDK](recovery-sdk/result.json)。`go vet ./...`、`GOWORK=off go build -mod=readonly ./...` 通过。

## 必须保留的失败与退化

1. 首轮发送/半帧测试遗漏 DATA Number，协议校验先拒绝，测试没有走到所需预算分支。保存 [round1](focused-round1.jsonl)，修正 fixture Number=1 后 round2 通过，未据此修改协议。
2. 初版资源驱动连接 receive=64 KiB，小于多个 16 KiB 流并发所需，宽预算也出现大量局部拒绝；runner 又把历史终态丢失引起的 drain 错误当成资源未归零。保存 [smoke 初版](resource-smoke/summary.json)。其中一个新版 QUIC tight 样本只有 `context deadline exceeded`，缺少阶段快照，当时无法确定阻塞点。后续加入阶段快照并再次实际复现，定位见下一节；没有增加业务重试。
3. 第二版驱动提高相同两版本的连接 receive 上限后，宽预算四 case 通过；tight 两个新版样本已回收全部字节/账户，但历史 LostTerminals 使 worker drain 返回 `kanarpc: terminal receipt not acknowledged`。原 runner 仍将其标 FAIL：[smoke round2](resource-smoke-round2/summary.json)。正式 runner 仅在明确相同错误、LostTerminals>0、所有实时资源归零时标 DEGRADED；保留 worker 退出码、错误、全部计数，其他失败仍是 FAIL。
4. 池接收耗尽会使 TCP/TLS 的整个有序字节流失去帧边界并替换连接，可能影响同连接其他请求；QUIC 的对应交换失败，父 session 可继续使用。三传输测试明确检查这项差异。控制额度独立不保证任意网络故障下 ACK 必达，也不消除已经发生的终态未回执。

## 压力复验发现并修复的 QUIC native stream 退休缺陷

正式资源矩阵的新版 QUIC tight case 在第一轮压力后恢复建流失败，原始失败保存在 [result](resources/current-quic-tight/result.json) 和 [阶段快照](resources/current-quic-tight/client.json.phases.jsonl)。池 Total.Used=0、调用数=0，仍不能证明底层 session 可复用。这次是 `OpenStreamSync` 等流额度，不是重新握手，也不是池 mutex 或编码配额没释放。

将负载缩成两个 loopback worker、8 个调用者、16 KiB 数据、3 秒混合普通/流式/取消，随后路由只保留一个 endpoint 并发起单块流。首次三轮 **1 PASS / 2 FAIL**：[复现](recovery-reproduction.jsonl)。失败等待期间 Encoding 持有已准备好的请求、EncodingWaiters=0、QUIC Active=1，但 Opened 和 Retired 不增加，3 秒后截止；不通过延时或重试绕过。

在隔离目录复制固定 quic-go v0.64.1，只添加只读诊断方法，不编辑 Go module cache/根依赖。在失败 session 上观察到：服务端仍保留 **64 条 incoming native stream**，接收方向均 completed；发送方向 reset/cancellation 已设置，outstanding=0、queuedReset=false，却都残留 nextFrame，发送方向未 completed。查看该固定版本 `SendStream.write`：并发 reset 清空帧后，Write 的尾部小帧优化可以再次缓存尾部，导致 completion 条件永远被 nextFrame 阻挡，最终耗尽远端 MAX_STREAMS 额度。[捕获](recovery-debug-buffers-repeat.jsonl)、[诊断源码/固定依赖片段及 hash](recovery-instrumentation.tar.gz)、[说明](recovery-instrumentation.json)。

最小修改仅将 `nativeChannel.Write` 改为公开的 `WriteWithLimit`，提供始终返回 maxBytes 的非阻塞 identity limiter，从而关闭这个小帧缓冲分支；没有 withholding、重试、依赖升级或 vendoring。客户端/服务端共用此适配。隔离副本三轮全通过，主代码 race 五轮全通过：[探针](recovery-workaround-probe.jsonl)、[race](recovery-after-race.jsonl)。随后四个完整矩阵各 266 项通过。固定 R8D 上将局部 receive 调成 24576 的三轮对照均通过：[负对照](recovery-r8d-local-budget.jsonl)；因此不声称所有旧版压力配置必然复现，也不把有限通过写成对底层缺陷的免疫证明。

该规避改变了 QUIC 发送缓冲行为，有性能成本，必须重跑成对 worker/client 的对照。第一轮固定旧 worker 的性能/资源结果仍保留，不能当作最终带服务端修复版本的验收。规避只针对已复现的这一分支，不宣称修复 quic-go 所有问题；字节租约和 RPC 计数也不涵盖库内部的全部持有。

修复后资源 runner 在完成 TCP roomy 后发生脚本变量 `workers` 覆盖（dict 被结果 list 覆盖），保存 [退出日志](resources-recovery-fixed.log)。改名为 worker_binaries 后，用相同 Go source hash 单独运行剩余三组；原始数据不改写，最终索引记录两次真实运行的位置。另有独立短矩阵验证 runner 本身。等待原矩阵的性能 gate 在任何 benchmark 启动前按自己持有的日志 FD 定位并停止：[回收记录](performance-gate-stop.json)。

## 有限资源与性能对照

最终资源数据跨两个真实执行目录保留，Go 源文件 hash 完全相同：[索引与退出错误](final-resource-index.json)、[源码对照收据](final-source-receipt.json)。TCP roomy 在 `resources-recovery-fixed` 完成，其余三组在 `resources-recovery-resumed` 完成；不把被脚本错误中断的运行写成一次完整成功。每组 12 个周期；宽预算 180 秒/组，饱和 60 秒/组，共 **480 秒最终版本活动负载、48 个周期**。R8D 基线为原矩阵中的 180 秒宽预算与 30 秒饱和，两个传输共 420 秒。饱和时长不同，不按 offered 数比较吞吐。

| 最终版本场景 | mixed offered / success / canceled | 周期空闲 FD / goroutine | Data 峰值 / 限额 | 最终累计账户 Created=Released | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| TCP 宽预算 | 2,344,522 / 1,758,392 / 586,130 | 9 / 7 | 476,317 / 4,194,304 | 25 | PASS |
| QUIC 宽预算 | 782,293 / 586,719 / 195,574 | 9 / 10 | 427,160 / 4,194,304 | 25 | PASS |
| TCP 饱和 | 1,389,052 / 71,721 / 38,385 | 8 / 6 | 81,966 / 131,072 | 283,831 | DEGRADED |
| QUIC 饱和 | 356,501 / 33,816 / 31,488 | 9 / 10 | 81,965 / 131,072 | 25 | DEGRADED |

总计 **4,872,368 次 mixed offered**，另有每周期两次专用未读/退休探针，不混入 mixed offered。最终四组每轮 idle Total.Used=0、退休账户=0，Close/Wait 后全部账户和字节归零，PeakAccounts≤2。已完成的未读探针在退休组真实持有约 16 KiB，取出/关闭前未提前归还。所有实时 server Connections/InFlight/Executing/NativeStreams/IncompleteExchanges 最终为零。

饱和失败全部保留：TCP 有 1,162,651 resource_exhausted、95,251 closed、21,044 unavailable；QUIC 有 291,197 resource_exhausted。两个 server 的累计 LostTerminals 合计分别 **257,426 / 56,071**，worker drain 返回错误，故为 DEGRADED。TCP 大量建连退休，QUIC 父 session 保持复用；这个故障范围差异同时有三传输回归验证。不能把“限制内存并大量拒绝”当成同等完成率下的内存或性能收益。首次正式矩阵的 QUIC tight 失败、两次 smoke 的 FAIL 和脚本中断仍分别保存。

强制 GC 后客户端 heap 区间：TCP 宽预算 1,078,392–1,112,256 字节、QUIC 宽预算 1,193,168–1,517,808 字节；饱和分别 1,074,280–1,094,512 与 1,165,104–1,267,184 字节。采样 FD/goroutine 固定不等于 RSS 硬上限、长期无泄漏或无库内部持有。资源阶段与正确性/工具检查可能共享主机 CPU，仅作有限资源观察；每 case 只有分钟级，不能写成单服务连续 15 分钟/小时级生产长测。

最终串行性能对照：TCP/QUIC × 普通/流式 × direct 两连接直接复用 / managed 两连接 × 三种子 × 两版本，**48 case / 48,000 offered 全成功**，无准备失败/重复执行/资源回收失败。各版本使用自己的配对 worker/client，捕获最终服务端写路径改动。GOMAXPROCS=2，8 个 closed-loop 调用者，64 次预热；p95 只统计成功请求。CPU 为三个进程的生命周期，含启动、预热和关闭。[最终摘要](performance-recovery-fixed/summary.json)、[所有 offered/结果](performance-recovery-fixed/raw-results.tar.gz)、[源码/二进制 hash](performance-recovery-fixed/metadata.json)、[差异](performance-deltas.json)。

| 传输/负载/策略 | req/s 旧→新 | 变化 | p95 ms 旧→新 | triple CPU s 旧→新 |
| --- | ---: | ---: | ---: | ---: |
| tcp/unary/direct | 54,919→50,018 | -8.92% | 0.2429→0.2617 | 0.1069→0.1069 |
| tcp/unary/managed | 49,913→51,953 | +4.09% | 0.2778→0.2754 | 0.1080→0.1099 |
| tcp/stream/direct | 11,235→12,060 | +7.35% | 1.2187→1.1218 | 0.3981→0.4136 |
| tcp/stream/managed | 13,101→11,772 | -10.15% | 1.0046→1.1206 | 0.4082→0.4331 |
| quic/unary/direct | 25,444→18,549 | -27.10% | 0.4971→0.5895 | 0.2018→0.2385 |
| quic/unary/managed | 24,498→21,313 | -13.00% | 0.5347→0.6261 | 0.2078→0.2436 |
| quic/stream/direct | 12,748→6,385 | -49.91% | 0.9419→1.8117 | 0.3926→0.8104 |
| quic/stream/managed | 12,823→6,041 | -52.89% | 0.9618→1.9739 | 0.3983→0.8194 |

**明显退化必须保留**：QUIC managed 流式吞吐下降约 **52.9%**，direct 也下降约 49.9%，p95/CPU 增加；managed 普通 QUIC 下降 13.0%，TCP 流式下降 10.1%。发送缓冲规避与这种代价相符，但本轮没有完整独立消融，不能将每项差异独占归因。TCP 普通 managed 出现 +4.1%、direct 流式 +7.3%，也不据此宣称普遍加速。只有三个短重复、共享主机背景负载未严格隔离，没有置信区间；最终对照与资源/本轮正确性任务串行，仍只是探索性 loopback fixture 数据，不是生产容量或推理性能。

字节租约微基准（GOMAXPROCS=2，每版三个短重复）也保留退化：旧局部 **57.82 ns / 16 B / 1 alloc**，新局部 **67.37 ns / 24 B / 1 alloc**，新共享 **76.39 ns / 24 B / 1 alloc**。显式 released 状态与池统计/锁有成本，不能把资源约束写成免费功能。[原始 benchmark 与摘要](benchmark-summary.json)。

修复前固定 R8D worker 的 48 case 性能轮次仍在 [先前摘要](performance/summary.json)，不能与最终配对 worker 版本合并取最好值。新的源码、累计 pass 数也没有覆盖或改写旧失败记录。验收结束时已核对最终资源/性能源码与工作区逐文件 hash 相同。性能逐 case 文件和全部 offered 无损压入各轮 raw-results.tar.gz，逐成员核验原始 SHA-256 后才移除散文件：[打包收据](packaging-receipt.json)。可用 `tar -xzf raw-results.tar.gz -C <空目录>` 查看原始 results.json 和全部 case；摘要、源码 capsule、hash、微基准和失败复现继续直接保留。

## 未解决风险与下一步三项

- 预算覆盖框架记录缓冲/编码预留，不能约束任意用户 codec 内部临时分配、所有管理对象/响应对象、TLS/QUIC 内部或 RSS。阻塞 codec、忘记 Close 的 Stream 会一直持有可见额度；本轮没有强杀用户 goroutine。
- 一个共享 mutex、每块未读更新和 O(活账户数) 快照有成本；控制额度独立但仍竞争统计锁。编码等待不是 FIFO/租户公平。`Rejected` 可由局部或池额度触发，不能只看一个累计计数推断全池已满。
- 默认池总额更严格，业务接受率可能下降；TCP 预算耗尽的故障范围、频繁重建、终态回执丢失与 sticky drain 错误均需要应用明确处理，不能自动重试已执行请求。
- 真实双机、物理 NIC/WAN、多成员 etcd、小时/天级负载、真实 GPU/vLLM 尚未验证。所有数据/取消/发现输入均为本地 fixture 或 loopback 验证，不是推理性能实测。CampusTrace 未修改。

如果继续面向生产，最值得的三项是：

1. 在真实双机和小时/天级受控负载下复验预算压力、QUIC/FD/退休收敛与 drain，采集终态丢失原因，区分应用取消与网络故障。
2. 对接收预算设计更早的有界接纳/信用分配与公平策略，减少过载时 TCP 连带失败和握手风暴；继续用当前 fail-fast/直接复用作基线，保留拒绝与延迟代价。
3. 优先用带并发 reset 回归的上游修复或安全写入策略恢复 QUIC 性能，再基于 profile 降低池锁/快照/每块统计与租约分配成本，补局部/池拒绝归因；继续对照直接复用和大/小 payload，避免只改善小请求演示。
