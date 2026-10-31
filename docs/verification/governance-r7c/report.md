# R7C：字节预算、方法调度与服务端压力反馈

2026-10-31。新模块 `github.com/KanaDoodle/KanaRPC`，darwin/arm64，Apple M5。按 [实施步骤与 API](../../governance-r7c.md) 完成 C1–C4。R7A/R7B 的流式、远端取消、ACK/FIN、deadline drain、安全传输及真实 etcd 保持回归；R7D CampusTrace/SDK 迁移尚未开始，日常大模型 API 路径不变。

这是独立编写的新版治理实现，没有复制旧衍生项目的调度或反馈代码。并发 AIMD 和方法加权轮转是通用控制思路，不宣称新算法。来源及依赖边界见 [ORIGINS.md](../../../ORIGINS.md) 和 [origins.json](origins.json)。R7B 源码基线保存为 [r7b-source.tar.gz](r7b-source.tar.gz)、[manifest](r7b-source-manifest.json)；没有改写旧项目的来源或历史。

## 实现内容与请求生命周期

1. 客户端预算从编码前开始；静态在途上限与可选的短效反馈接纳共同检查。通过后注册 ID、写 OPEN，失败不重试、不降级、不重放。
2. 接收端验证帧头，再从共享池预留正文。Server 所有连接/QUIC 子流共享字节和执行预算；公共 QUICClient 的子流共享自己的接收/发送/反馈状态。
3. 服务端封闭状态、在途、每物理连接、方法及全局执行约束共同接纳；无槽时进入有限队列。方法加权轮转或 work-conserving FIFO 选最早可执行请求，排队取消/超时不调用业务。
4. 真正调用处理器前再次检查 Context/deadline。执行期间 DATA 消耗信用，正文与合并包分别计费；控制记录使用独立字节池。取消不需要先取得执行名额。
5. 处理器返回后释放执行名额、更新方法窗口 p90/AIMD、调度下一请求，再发送可选压力元数据与 END。终态 ACK/FIN 等待继续占在途名额，执行时间不包含这段等待。
6. 客户端校验反馈版本/路径/epoch/方法序列/TTL/数值范围；完成通知前释放本地方法名额。结束未读的块继续计费，消费/流 Close/公共父 Close 才释放。
7. drain 先封闭新接纳，允许已接纳队列继续执行，再等待 ACK/FIN 退休。截止后回收 IO 并取消，仍不能强制杀死忽略 Context 的处理器；统计保留未退出工作。

`MemoryStats()` 是有界租约而非 RSS；客户端 JSON 编码缓冲、应用对象、TLS/QUIC 内部与 allocator 不在预算内。客户端编码早于在途接纳，很多调用者等待 writer 时仍可能持有未计费编码结果。方法公平也不是租户/token/CPU 成本公平。完整配置和边界详见实施文档。

## 复现、根因和修复

以下包含既有预算/终态尺寸边界和本轮新增实现初稿中的缺陷，不应包装为上游漏洞统计。失败日志、无效测试夹具及通过记录均保留。

| 实际问题与最小复现 | 根因 | 修改及验证 |
| --- | --- | --- |
| 未预算 reader 仅收到四个 1 MiB 长度声明，即申请约 4 MiB payload；[allocation-baseline.txt](allocation-baseline.txt) | 只有逐帧上限，在读完头后立即分配；并发子流的乘积没有总边界 | ReadReserved 分配前预留；2 MiB 服务端预算下首个 1 MiB OPEN 按两倍计费，其余三个被拒，reset 后回收且健康调用仍可用。是 payload 分配观察，不是 RSS 或完整旧版负载测量 |
| 极大 TTL 整数被接受；[governance-initial.jsonl](governance-initial.jsonl)、[feedback-boundary-before.jsonl](feedback-boundary-before.jsonl) | 先把毫秒乘成 time.Duration，溢出后绕过时间上界 | 先做整数毫秒范围校验，再转换；边界 fuzz 和回归通过 |
| 迟到 80ms 的 50ms 建议到达后又获得完整 TTL；[feedback-boundary-before.jsonl](feedback-boundary-before.jsonl) | 用本地接收时间作为完整有效期起点，忽略已有调用年龄 | 保守从 TTL 扣除整个本地调用年龄，再按单调时间过期；长调用可能丢弃刚生成的建议，明确保留这个取舍 |
| 调用已发布完成，但反馈 Active 仍为 1，下一次调用误拒；[feedback-ticket-before.jsonl](feedback-ticket-before.jsonl) | 名额释放只在 IO 退休清理，晚于完成通知 | pending.finish 在通知前幂等释放；暂停清理的确定性 fixture 及跨 QUIC 交换测试通过 |
| 某方法占满队列，另一个方法有空闲执行槽仍被拒；[dispatch-before.jsonl](dispatch-before.jsonl) | 接纳只看公共队列是否满，没有检查该方法可立即执行 | 队列满且满足方法/全局执行约束时直接执行；不跳过更早的可执行积压，最小用例通过 |
| 获得执行槽后、goroutine 调用前已过期，业务仍被调用；[dispatch-before.jsonl](dispatch-before.jsonl) | 只在排队及分配槽时检查，调度间隙缺少最终检查 | 在 invoke 前检查 Context 和绝对本地 deadline；确定性构造过期已调度请求，业务调用计数为零 |
| 取消后队列长度归零，但底层数组仍引用请求图；[boundaries-before.jsonl](boundaries-before.jsonl) | append 移除元素后没有清空尾槽 | copy 后清空最后指针再缩短；真实排队取消后直接检查原 backing array，普通/race 通过；不是无限增长泄漏证明 |
| 方法策略拼错路径静默忽略；[boundaries-before.jsonl](boundaries-before.jsonl) | 配置在构造期校验格式，但未在注册冻结时核对方法存在 | TCP/TLS/QUIC Serve 在接管 listener 前核对注册表，错误包含方法路径并保留 fresh 状态 |
| QUIC 关闭屏障收到超预算异常 DATA，未计费读取；[boundaries-before.jsonl](boundaries-before.jsonl) | SESSION_BARRIER 的专用 reader 仍调用无预算 Read | 关闭控制流的收发同样预留共享预算；真实 QUIC 1 KiB 预算接收异常 2 KiB DATA，拒绝且 Peak=Used=0 |
| 合法 256 字节错误诊断在 JSON 转义后超过 1 KiB，TCP/QUIC 丢失结构化状态；[terminal-size-before.jsonl](terminal-size-before.jsonl) | 仅限制原始 UTF-8 消息长度，没有按实际 END 编码大小计入压力字段和转义放大 | 编码后按帧/控制限额缩短可选消息，保留 UTF-8、状态码、回执，必要时去掉可选压力；两种传输能收到 InvalidArgument 并继续健康调用，[terminal-size-after.jsonl](terminal-size-after.jsonl) race 十轮通过 |

对应通过记录：[control-after.jsonl](control-after.jsonl)、[dispatch-after.jsonl](dispatch-after.jsonl)、[boundaries-after.jsonl](boundaries-after.jsonl)、[native-control-race.jsonl](native-control-race.jsonl)。初稿中反馈 fixture 未读取 CREDIT，导致 net.Pipe 写超时；另一次复现测试用了错误 handler 类型导致编译失败。这些分别保留为 `*-fixture-error.jsonl` / `*-fixture-build-error.jsonl`，不计作框架缺陷；修正夹具后重新取得确定性失败再修实现。

新增验证还覆盖完成未读块的 TCP/QUIC 计费、Stream.Close/Recv/父 Close 回收、业务发送字节耗尽后控制终态仍可发送、幂等租约释放、队列过期/取消无业务调用、终态等待不占执行槽、权重 2:1 的持续积压启动比例、空闲连接不分摊压力、反馈乱序/过期/epoch/容量及 QUIC 跨交换共享接纳。合成 AIMD 输入只证明界限和决策逻辑，不是实测控制收益。

## 最终检查

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| Go 1.27.1 普通，独立真实 etcd | 144 pass / 0 skip / 0 fail | [verified-test/result.json](verified-test/result.json)、[test.jsonl](verified-test/test.jsonl) |
| Go 1.27.1 race，独立真实 etcd | 144 pass / 0 skip / 0 fail | [verified-race/result.json](verified-race/result.json)、[test.jsonl](verified-race/test.jsonl) |
| Go 1.26.0 普通，独立真实 etcd | 144 pass / 0 skip / 0 fail | [verified-min-test/result.json](verified-min-test/result.json) |
| Go 1.26.0 race，独立真实 etcd | 144 pass / 0 skip / 0 fail | [verified-min-race/result.json](verified-min-race/result.json) |
| go vet | PASS | [vet.txt](vet.txt) |
| 带预算帧读取 fuzz，15s / 2 workers | 3,428,796 executions，PASS | [fuzz-FuzzReadReserved.jsonl](fuzz-FuzzReadReserved.jsonl) |
| 字节租约 fuzz，15s / 2 workers | 494,110 executions，PASS | [fuzz-FuzzByteBudget.jsonl](fuzz-FuzzByteBudget.jsonl) |
| 压力元数据 fuzz，15s / 2 workers | 669,089 executions，PASS | [fuzz-FuzzPressure.jsonl](fuzz-FuzzPressure.jsonl) |
| 终态 JSON 转义与帧界限 fuzz，15s / 2 workers | 661,132 executions，PASS | [fuzz-FuzzTerminalEncoding.jsonl](fuzz-FuzzTerminalEncoding.jsonl) |
| 双进程 SIGTERM、流式、取消、drain 回归 | PASS | [core-regression/result.json](core-regression/result.json) |
| 新旧默认/协作取消与 drain 行为对照 | 18 轮通过，非性能测试 | [behavior-regression/comparison.jsonl](behavior-regression/comparison.jsonl) |
| TCP/TLS/QUIC 普通/16 块流回归 | 18 组，18,000 次 offered 全成功 | [transport-regression/summary.json](transport-regression/summary.json) |
| 实际治理双进程 race 二进制 | TCP/TLS/QUIC 各一组，共 4,200 次 offered，预期限流/超时允许，资源检查通过 | [lab-race/summary.json](lab-race/summary.json)、[原始记录](lab-race/cases.tar.gz) |
| 最终开放到达对照 | 66 组，108,000 次 offered，66 组验收通过，包含下列拒绝/超时 | [summary.md](comparison-final/summary.md)、[analysis.json](comparison-final/analysis.json) |

144 包含子用例和 fuzz seed，不是 144 种独立功能。真实 etcd 是本机 3.7.1 单成员服务，客户端/api 依赖仍为 3.6.7；每轮先确认唯一 member/name/client/peer URL 属于本任务，才运行 compaction，结束回收自己的进程。没有使用既有 etcd 服务或修改其他数据库。四轮矩阵串行，在最终 END 尺寸修复后全部重跑；此前每轮 137 项的日志保留在 [staged-pre-terminal](staged-pre-terminal)。有限 race/fuzz 不能证明不存在所有并发或泄漏缺陷。

最终一致性检查见 [verification.json](verification.json)，当前源码包见 [source.tar.gz](source.tar.gz)，证据文件哈希见 [SHA256SUMS.json](SHA256SUMS.json)。源码包按 origins 清单构建；最终测量所记录的全部 Go/go.mod/go.sum 哈希与工作目录一致。

## 最终负载：方法公平、自适应及拒绝代价

最终 Go 源码和二进制固定后串行测量；本任务未并行运行其他测试。两个独立进程、GOMAXPROCS=2、4 条客户端物理连接、每个策略/场景 3 个种子（7/19/41）、每轮开放到达 2s、生成器同时最多 128 调用，晚于计划 50ms 的 offer 会单独记录丢弃。没有业务重试，也不把本地拒绝移到下一轮。处理器为真实 RPC 包裹的定时器/动态槽位 fixture，128 字节 payload；没有模型。

五种策略都启用相同服务端压力元数据，控制元数据开销相同。仅 `feedback` 开启客户端本地建议接纳。固定高并发方法上限为 16；静态低并发参照 `fixed4-fair` 的慢方法固定为 4。所有组全局执行槽 16、队列 64、队列等待上限 60ms。自适应初始 16、慢方法最低 2、窗口 16；无预热，保留启动与目标切换期。没有穷举静态并发参数，也没有已知每段容量的理想 oracle，不能将本轮对照称为超过最佳静态配置。

| 场景 | fast/slow 到达率 | 慢处理器与容量 | 控制目标 |
| --- | --- | --- | --- |
| 低负载 | 10/10 每秒 | 4ms，8 槽 | 慢方法 10ms |
| 稳态 | 200/1200 每秒 | 4ms，8 槽 | 慢方法 10ms |
| 容量切换 | 200/1200 每秒 | 4ms，8→2→8 槽，每段约 2/3s | 慢方法 10ms |
| 目标不可达 | 20/200 每秒 | 固有 20ms，4 槽 | 慢方法 **5ms** |

快处理器约 1ms，控制目标 4ms。槽位是真实互斥的 fixture 接纳门，目标是实验参数，不是生产最优参数。业务对照“按时完成”另用从**计划发送时刻**起快≤20ms、慢≤50ms：包括生成器和 goroutine 调度迟到；与控制器的处理器驻留目标不同。成功分位数只含成功请求，必须同时看 offered/拒绝和按时完成数。CPU 是两个进程全部生命周期 CPU，包含初始化、负载生成、admin 采样与关闭；不是 RPC-only ns/op。

容量切换场景，各策略合计 8,400 次 offered（快 1,200、慢 7,200）；延迟是三轮成功请求 p95 的算术平均：

| 策略 | OK | 快 OK | 快 p95 ms | 按时完成总量 | 本地反馈拒绝 | ResourceExhausted | deadline 错误 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fixed-fifo | 6,974 | 993 | 60.547 | 5,804 | 0 | 587 | 839 |
| fixed-fair | 7,156 | 1,152 | 4.420 | 6,143 | 0 | 330 | 914 |
| fixed4-fair | 6,220 | 1,200 | 1.309 | 1,861 | 0 | 755 | 1,425 |
| adaptive | 7,157 | 1,200 | 1.286 | 6,070 | 0 | 282 | 961 |
| feedback | 6,897 | 1,200 | 1.301 | 6,884 | 1,503 | 0 | 0 |

固定方法公平本身已把快请求成功 p95 从 60.547ms 降到 4.420ms；不能把这部分收益归给自适应。仅服务端自适应进一步保住全部快请求，但按时完成总量 6,070 还略低于固定公平的 6,143。反馈辅助相对仅服务端自适应，按时完成 **+13.4%**，OK 总量 **-3.6%**，代价是 1,503 次提前拒绝。该配置实现了更早放弃部分慢请求，不能说提升所有请求吞吐或保证 SLO。

## 必须保留的退化

目标不可达场景，各策略合计 1,320 次 offered（快 120、慢 1,200）：

| 策略 | OK | 按时完成 | 本地反馈拒绝 | deadline 错误 | 最终慢方法上限 |
| --- | ---: | ---: | ---: | ---: | --- |
| fixed-fifo | 1,318 | 985 | 0 | 2 | 16 |
| fixed-fair | 1,316 | 985 | 0 | 4 | 16 |
| fixed4-fair | 1,317 | 850 | 0 | 3 | 4 |
| adaptive | 889 | 421 | 0 | 431 | 2 / 2 / 2 |
| feedback | 828 | 822 | 492 | 0 | 2 / 2 / 2 |

慢方法固有耗时约 20ms，5ms 目标无论降多少并发都不可达，AIMD 持续降至 Min=2。相对固定公平，自适应完成量 **-32.4%**、反馈辅助 **-37.1%**；固定低并发 4 也明显优于两种自适应的完成量。提前拒绝减少了等待，但无法弥补错误目标引起的容量损失。当前没有目标可达性识别或自动恢复到已知安全静态配置，这项设计风险没有掩盖或声称已解决。

低负载每策略 120 次都成功，无吞吐提升证据；成功 p95 在约 1.5–1.6ms 波动。稳态固定高并发/公平、自适应、反馈辅助各 8,400 次全成功；固定低并发只有 7,172 次成功，说明静态低值也会限制健康容量。稳态反馈辅助进程对 CPU 均值 0.760s，固定公平 0.855s、仅服务端自适应 0.717s；相对不同基线方向不同，没有普遍 CPU 收益。每组仅三次、短时 loopback，无置信区间，不能用这些差异宣称生产显著性或普遍最优。

额外 TLS/QUIC 组只验证同一反馈链路的传输兼容：各 8,400 次 offered，分别 6,867 / 6,936 成功，其余为预期接纳拒绝；不是本轮公平的跨传输速度对照。R7B 历史性能结论仍属于其原快照；本轮传输回归的短时性能数据不用于重新宣称 QUIC 增益。

最终所有场景累计：**95,247 OK、4,992 本地反馈拒绝、5,092 deadline 错误、2,669 ResourceExhausted**，合计 108,000；没有 generator_late/generator_limited、响应损坏、未分类传输错误或重复业务 ID。全部 case 的连接、排队、执行、在途、原生流、丢失终态、未完成交换归零，字节 Used=0 且 Peak≤Limit，客户端反馈 Active=0，自有进程全部回收。这是有限资源尾部验证，不能替代长期 goroutine/heap 曲线。

原始 offered、带时间戳状态采样、逐轮决策、日志和公用 CA 保存在 [cases.tar.gz](comparison-final/cases.tar.gz)，source/binary hash 在 [metadata.json](comparison-final/metadata.json)，汇总及种子间 p95 保存在 [analysis.json](comparison-final/analysis.json)。没有保存私钥。初次 1s [pilot](pilot/summary.md) 使用早期快照，之后修正了调用 goroutine 调度迟到的计时；它只保留为试跑。终态尺寸修复前完整的 66 组保存在 [comparison-before-terminal-bound](comparison-before-terminal-bound/summary.md)，含与当时 hash 一致的 [Go 源码快照](comparison-before-terminal-bound/source.tar.gz)；修复后完整重跑，上述主表只采用最终一轮。两轮差异不能全部因果归于该尺寸修复。实际 race 进程的时间也不作性能证据。

## 尚未解决的设计风险

- **目标与震荡**：没有目标可达性检测、业务成本分组、探测窗口、稳定性证明或动态参数优化。窗口包含处理器内依赖/信用等待；慢消费者也可能让控制器降并发。目标错误会持续压到 Min，调参不等于修复这个设计问题。
- **公平与反馈归属**：只有服务端配置的方法启动权重，所有方法共享队列，仍可被一个方法占满；没有租户身份、每租户队列、token/CPU 成本、公平等待界限或抗多连接分摊。建议至少为 1，多连接会超卖；它是 advisory，不是分布式容量租约。
- **内存和长期资源**：预算范围有限，尤其客户端编码先于在途接纳。没有全进程内存上限、长时间 goroutine/heap/GC 曲线或持续恶意负载验收；同步编码、解码和用户处理器不可抢占。任意应用并发数量仍应受上层约束。
- **网络与控制面**：未验证跨机 RTT/丢包/重排/受限带宽、迁移或生产负载。TCP 连接仍有共享坏帧/写阻塞影响，控制池不能隔离 CPU/写锁/连接窗口。etcd 仅验证独立单成员，无多节点选主矩阵；没有发现驱动池或业务重试。
- **兼容与信任**：可解析但语义无效反馈会忽略，完全损坏的 END/JSON 仍属协议错误。明文 TCP 不认证服务身份，TLS/mTLS 也不提供方法 ACL；epoch/sequence/TTL 不是签名或权限票据。压力只在响应时回传，初始突发和长流缺乏及时反馈。
- **应用和真实模型**：新版 SDK/CampusTrace 接入、权限/重复调用/UNKNOWN 恢复仍属于 R7D。真实 vLLM/GPU、真实模型 API 推理性能均 **NOT_VALIDATED**；所有本轮业务都是定时器/动态槽位 fixture，不代表真实模型缓存命中、token 吞吐或推理收益。

## 下一步最值得做的三个地方

1. **R7D 独立 SDK 与 CampusTrace 迁移**：保持日常模型 API、实验 vLLM 可选；用新旧端点分开验收流式、取消、权限、重复调用和现有任务恢复，再决定切换依赖。
2. **控制器可靠性**：针对已经复现的不可达目标退化，先设计自然静态基线、目标可达性/回退契约与长时阶跃对照；建立真实身份及成本后，再实现租户隔离和公平队列。
3. **资源与真实网络证据**：编码前并发接纳/编码字节边界、长期 goroutine/heap/GC 压力曲线，以及 Linux 双机 TCP/TLS/QUIC 的 RTT/丢包/带宽矩阵；真实推理后端单独验收，不能与 fixture 报告合并。
