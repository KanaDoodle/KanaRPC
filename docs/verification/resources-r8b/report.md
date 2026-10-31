# R8B：编码前资源接纳与资源观察

2026-10-31，darwin/arm64，本机 loopback；只修改新版 KanaRPC。实施步骤和公开 API 见 [resources-r8b.md](../../resources-r8b.md)。R8A 基线为 d595a2d7369e50cd883b53bcdcee6312d55953a5；[原源码](r8a-source.tar.gz)、[manifest](r8a-source-manifest.json)保留。CampusTrace 与旧 KanaRPC-Go 未修改。

## 复现、根因和修复

| 实际复现 | 根因 | 修改与回归 |
| --- | --- | --- |
| MaxInFlight=1 下 16 个阻塞编码器全部进入；关闭 TCP 客户端和反馈拒绝仍运行编码器 | 名额、关闭与压力检查位于 json.Marshal 之后 | 编码前共享接纳票据，覆盖准备、等编码容量/写许可、pending 和 QUIC 物理收尾；拒绝不进入编码器 |
| 慢 writer 前已编码正文未在 MemoryStats 计费 | SendBytes 仅约束真正发送包，未约束准备结果 | EncodingBytes 编码前预留两帧，成功后缩减并覆盖保留正文与 OPEN 复制；等待写许可期间持续计费；同步编码不派生后台 goroutine |
| QUIC 父连接已知结束，后续调用仍编码 | 公共根客户端准备没有观察 parent Context | 父连接结束封闭共享接纳并取消准备；在已知失效连接上编码次数为 0 |
| 初稿 Close 取消 owner 后，部分 pending/编码等待返回 context.Canceled | 次生取消抢先覆盖显式关闭原因 | 先发布 pending 原因，再取消 owner；准备/许可等待映射显式关闭为 ErrClosed |
| 初稿并发小请求突发在编码容量不足时立即拒绝 | 两帧保守预留配合 fail-fast，改变了正常突发行为 | 在 MaxInFlight 限定的调用者内有界等待容量；超时/取消/Close 可退出；不另起编码任务 |

原失败见 [before](before.jsonl)、[dead-parent-before](dead-parent-before.jsonl)、[initial](initial.jsonl)、[resources-build](resources-build.jsonl)、[preparing-close-before](preparing-close-before.jsonl)。修复回归见 [preparation-after](preparation-after.jsonl)、[close-cause-after](close-cause-after.jsonl)及最终全量矩阵。不是把新增正向断言全部计成漏洞。

新增测试还覆盖 writer 前取消、超大/无效 JSON、编码 panic 与成功请求恢复、QUIC 编码前零原生流、用户编码阻塞期间 Close/WaitIdle、同步用户函数释放后的名额和字节归零。Close 不能强杀任意用户 MarshalJSON；WaitIdle(ctx) 如实暴露残留。已结束未读流仍由 Receive 字节预算单独计费。

## 正确性验证

下表 175 项是编码接纳检查点。后续真实故障又确认父 QUIC 取消会丢失容量终因，本轮已修复：仅将框架产生的 ErrClosed/context.Canceled 映射回父连接 Cause；用户取消和显式 Close 优先，包装的 json.MarshalerError 保持原身份。[终因失败](quic-cause-before.jsonl)、[50 轮 race](quic-cause-after.jsonl)、[包装错误失败](quic-codec-before.jsonl)、[修复回归](quic-codec-after.jsonl)均保留。原容量拒绝用例失败的完整矩阵在 [R8C 记录](../connections-r8c/verified-test/result.json)。

包含所有最终修复和 R8C 的 Go 1.27.1 / 1.26.0 普通、race 四轮均 **213 pass / 0 skip / 0 fail**：[1.27 普通](../connections-r8c/release-test/result.json)、[race](../connections-r8c/release-race/result.json)、[1.26 普通](../connections-r8c/release-min-test/result.json)、[race](../connections-r8c/release-min-race/result.json)。旧反馈夹具的一次私有锁等待环另已复现并修正同步顺序，详见 R8C 报告；没有据此修改运行时协议。

| 检查 | 结果 |
| --- | --- |
| Go 1.27.1 普通 / race，真实隔离 etcd | 各 175 pass / 0 skip / 0 fail：[普通](verified-test/result.json)、[race](verified-race/result.json) |
| Go 1.26.0 普通 / race，真实隔离 etcd | 各 175 pass / 0 skip / 0 fail：[普通](verified-min-test/result.json)、[race](verified-min-race/result.json) |
| 准备与关闭用例 race 重复 | PASS；显式关闭原因另重复 20 轮 |
| FuzzPreparedOpen，15s 配置、2 workers | 341,066 executions，PASS：[日志](fuzz-prepared.jsonl) |
| go vet | PASS：[记录](vet.txt) |

175 含子用例与 fuzz seeds。etcd 3.7.1 单成员真实 lease/watch/compaction，拥有并回收唯一实例；不代表生产多成员验收。较早普通/race 矩阵与中断资源运行在 [归档](earlier-matrix-and-interrupted.tar.gz)。

**未掩盖不稳定结果**：一次完整 race 在 TestNativeQUICImmediateCloseConfirmedAfterTerminalReceipt 的初始化 Dial 阶段发生 handshake timeout；随后的 5 轮复跑首轮也失败，后 4 轮通过。原日志见 [失败矩阵](final-go127-race/result.json)、[复跑](quic-handshake-repeat.jsonl)。相同握手用例在 R8A 与 R8B 分别再做 10 轮均通过：[基线](handshake-r8a-repeat.jsonl)、[当前](handshake-r8b-repeat.jsonl)。完整矩阵最终重跑通过，但两次初始化超时根因未被确定，不声称已经修复或证明与本轮无关。

## 串行性能对照与退化

先冻结 [最终源码](final-performance-sources.tar.gz)和[源码 hash](final-performance-metadata.json)，并记录[传输二进制 hash](transport-binary-hashes.json)，新旧版本注入同一 resource-lab；对照运行期间没有并行 Go 测试。性能由相同 transport-lab 独立 worker/client 产生，GOMAXPROCS=2、8 个闭环调用者、种子 7/19/41，TCP/QUIC × 普通/16 块流 × 新旧，共 24 case、72,000 offered 全成功、无重复执行，worker/drain 尾部资源为零。[最终摘要](final-performance-summary.json)、[完整 offered 和日志](final-performance-raw.tar.gz)。

每格为三个短重复的中位数。p95 只统计成功调用；CPU 是两个进程全生命周期，包括 warmup/启动/关闭，无置信区间。这里是整数和固定 JSON fixture，无模型。

| 传输/负载 | req/s，旧 → 新 | p95 ms，旧 → 新 | pair CPU s，旧 → 新 |
| --- | ---: | ---: | ---: |
| tcp/unary | 40,057 → 39,482 (-1.43%) | 0.3161 → 0.3213 | 0.2450 → 0.2460 |
| tcp/stream | 8,076 → 8,100 (+0.30%) | 1.4063 → 1.3998 | 1.1948 → 1.1995 |
| quic/unary | 22,534 → 22,793 (+1.15%) | 0.5426 → 0.5357 | 0.3996 → 0.4049 |
| quic/stream | 10,481 → 10,705 (+2.14%) | 1.1354 → 1.1007 | 0.8829 → 0.8572 |

最终 TCP 小普通请求吞吐下降 1.43%，p95 和 CPU 增加；QUIC 普通请求吞吐增加 1.15%，CPU 也增加。TCP/QUIC 流式分别增加 0.30%/2.14%。短时本机重复不能把这些差异归因于单一锁/分配，也不能宣称资源治理有普遍性能收益。新接纳的目标是阻止超预算准备工作，承担统计、同步与保守预留的代价。

## 资源曲线

先做新旧 TCP/QUIC 各 60 秒、六个负载/清理周期（共 240 秒活动观察），全部通过：[摘要](resource-60s-summary.json)。阻塞编码实验为 64 个并发调用、MaxInFlight=8，每个用户编码器可自行分配 64 KiB；不是推理性能或任意编码内存硬限额实验。

- TCP：进入编码器从旧 64 降为新 8，新版 56 个调用在编码前被 ResourceExhausted 拒绝。阻塞时 client heap 约 4.26 MiB → 0.70 MiB；这个特定编码器的比较不能推广为全进程峰值保证。
- QUIC：两版进入编码器均为 8，但阻塞期间原生业务流 Opened 从旧 8 降为新 0。拒绝/编码失败不再占用业务流。
- Close 返回时，已进入用户编码器的工作仍计费；释放用户函数后新 Active/Preparing/Encoding/EncodingWaiters 和全部池 Used 均为零。
- 每轮额外持有 8 条已完成未读流，确认 Receive.Used 为正，然后关闭并确认计费归零。全量业务/取消/未读混合负载不保留随请求数增长的业务 ID map。

延长观察与完整曲线见下方追加记录。原 60s 观察和较早性能轮次的源码、hash、原始数据均保留为阶段记录，最终性能结论只使用上面最终串行轮次。资源曲线是明确负载和时长下的有限样本；goroutine 稳定和计费归零不等于证明 Go allocator、TLS/QUIC 内部、应用对象或 RSS 永不增长。

## 尚未解决与下一步

1. 任意用户 MarshalJSON/UnmarshalJSON 可阻塞、panic、临时分配巨大对象；同步执行不能保证配置 deadline 一到就返回。EncodingBytes 约束有效结果和记录复制，不能写成用户内部/RSS 上限。保守两帧预留也可能减少并行编码、增加等待。
2. 此处只有分钟级本机资源观察；小时/天级压力、Linux 双机、RTT/丢包/限带宽、慢消费者、etcd 多成员故障属于 R8D。握手初始化偶发超时仍待定位。
3. 本轮随后实施 R8C 连接管理，必须保护已完成未读流，给全部 dialing/retiring/closing 计费，并坚持不自动重放业务；真实 vLLM/GPU 未验证，不能将 fixture 当推理收益。

## 延长资源观察与观察器修正

第一轮新旧各 180s/12 周期虽计费清零、goroutine 稳定，client heap 却随周期增长。检查发现观察器自身在 client 中累计 samples 和 phases；旧 TCP 从 327,312 到 585,232 字节，新 TCP 从 382,144 到 556,512 字节，同时保留 180 个样本和 12 个阶段。这个测量干扰已确认存在，不能把全部增长归因于 RPC。[受干扰曲线](observer-confounded-summary.json)、[原始数据](observer-confounded-180s.tar.gz)保留。

观察器改为逐样本/逐阶段写 JSONL，Python 在测量进程退出后才加载历史；使用同一修正 driver 对新旧重新运行 TCP/QUIC 各 180 秒、12 周期，共 720 秒活动窗口和 48 个清理点：[摘要](streamed-summary.json)、[CSV 曲线](resource-curves.csv)、[原始 JSONL/日志](streamed-raw.tar.gz)、[hash](streamed-metadata.json)、[源码](streamed-sources.tar.gz)、[当时的驱动](resource-driver-at-soak.py)。该过程与本机正确性验证存在主机 CPU 重叠，只解释每进程资源与回收，不比较吞吐/时延。性能对照单独串行运行。

| 版本/传输 | 混合操作完成数 | client GC 后 heap 范围 KiB | client goroutine | server goroutine |
| --- | ---: | ---: | ---: | ---: |
| baseline/tcp | 3,647,403 | 320.9–379.5 | 4 | 7 |
| current/tcp | 3,630,782 | 295.2–346.9 | 4 | 7 |
| current/quic | 1,061,316 | 465.6–668.7 | 7 | 13 |
| baseline/quic | 1,005,154 | 533.6–708.2 | 7 | 13 |

操作完成数含按计划取消的流，不等于每个 RPC 都收到业务 OK。每轮保留未读流时 Receive.Used 为正，关闭后所有池 Used=0、Peak≤Limit；最终 server Connections/InFlight/Executing/NativeStreams/LostTerminals/IncompleteExchanges 为零。goroutine 数稳定；heap 有波动，在此窗口没有观察到按周期单调累积，仍不能排除低速泄漏或推导 RSS 上限。CSV 时间轴为名义活动窗口，不包括预热和阶段清理。

上述长测源码冻结在最后的 QUIC 终因修复之前，资源计费实现相同。为避免把旧检查点冒充最终源码，终因修复后另外冻结并重跑最终 TCP 60s/6 周期、QUIC 180s/12 周期，分别完成 1,220,051 / 934,541 个混合操作，全部回收检查通过：[最终补测](post-cause-summary.json)、[源码/二进制 hash](post-cause-metadata.json)、[原始记录](post-cause-raw.tar.gz)。这同样只提供有限时长的资源观察。
