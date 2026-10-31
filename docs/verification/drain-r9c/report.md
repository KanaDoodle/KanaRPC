# R9C：过载、取消、未回执与 drain

从 R9B `0b33dad` 继续，仅修改 KanaRPC。实施前的步骤见 [计划](../../drain-r9c.md)。没有做 gRPC 对照，没有修改 CampusTrace，没有真实双机、物理网络或真实 vLLM 验证。

## 已确认的问题、复现与根因

| 问题 | 最小复现 | 根因与修改 |
| --- | --- | --- |
| 本机配额拒绝与业务 ResourceExhausted 无法区分 | 1024B send 额度申请 1025B；错误类型与业务手写 Status 相同 | byteBudget 仅返回 Status。新增本机 BudgetError，提供类别、拒绝层级与 Requested/Used/Limit；Unwrap 仍提供原 ResourceExhausted Status，线上协议不变 |
| END 后的取消/请求期限缺少结算解释，旧 Canceled 又混合业务状态与上下文结束 | 读取带 receipt 的 END 后发送 CANCEL、不 ACK；旧 Completed=1，Delivered/LostTerminals/Canceled 均为 0 | 旧 Canceled 只在 handler 返回时按状态码更新，recordDelivery 对取消和 deadline 不记录。新增累计 Outcomes；明确首次 CANCEL 观察与回执结算分开，截止时间包括 handler 返回后的等待期 |
| drain 的 context 错误掩盖已有交付故障 | 先制造一次真实无 ACK，再挂起一个忽略取消的 handler，使用已取消的 Context drain | 原 Drain 超时分支只返回 ctx.Err。新增 DrainWithReport，保留累计故障和资源状态；同时存在两个错误时 errors.Join，原 Drain 保留入口 |
| 拒绝响应的写预算失败丢失实际原因 | 已接纳 handler 等待，耗尽独立控制发送额度，再 OPEN 未注册方法 | open 返回真实写错误，handleFrame 却只返回 false，readLoop 统一关闭原因。现在在该分支按真实错误关闭，区分本机预算中断与其他通道故障 |

修改前首组三个反例 **3 FAIL**：[原始输出](before.jsonl)，同组实现后 **3 PASS**：[回归](after-initial.jsonl)。控制响应反例先修正了夹具配置（控制预算至少 4096B），随后确定性失败：[真实反例](control-cause-corrected-fixture-before.jsonl)，修复后 **20 次 race PASS**：[回归](control-cause-after-race.jsonl)。

最终版本可直接运行以下命令复核；首组 drain_boundary_test.go 不依赖新增 API，也可复制到隔离的 `0b33dad` checkout 运行，预期三个测试失败，不需要改动当前工作区。

```sh
go test -count=1 -run '^TestBoundary' .
go test -race -count=20 -run '^TestRejectControlBudgetAbort' .
go test -race -count=10 -run 'Test(DrainBoundary|DrainReport|DuplicateCancel|ReceiptSettlement)' .
go test -run '^$' -fuzz '^FuzzReceiptSettlement$' -fuzztime=10s -parallel=2 .
python3 scripts/verify-discovery.py --race --output /tmp/kanarpc-r9c-race
python3 scripts/verify-sdk.py --output /tmp/kanarpc-r9c-sdk
python3 scripts/verify-pool.py --baseline 0b33dad --output /tmp/kanarpc-r9c-resources --duration 60 --tight-duration 30 --cycles 6
python3 scripts/compare-quic-write.py --versions r9b current --output /tmp/kanarpc-r9c-perf --payloads 128 16384 --transports tcp quic --workloads unary stream --strategies managed --seeds 7 19 41 67 97 --count 1000 --workers 8
```

脚本输出目录必须尚不存在。性能命令应在其他本轮检查退出后单独执行；资源 runner 的顶层 0 退出只表示实验完整且资源断言通过，饱和 worker 的 ErrDelivery 和 DEGRADED 仍在 summary 中保留。

这些结论包括可诊断性缺口，不把“缺少计数”描述成业务已经错误执行。回执结算增加互斥截止点与幂等保护：结算前已观察 ACK 时优先；迟于结算的 ACK 不抹去历史故障。未新增网络重试或消息帧，也未更改 R9B 的 QUIC 有界写适配器、池级原子接纳和退休机制。

## 行为边界

- 预算失败、取消观察、交付结果是可以重叠的观察轴。已接纳请求发送预算不足，可返回带 receipt 的 ResourceExhausted END 并正常 ACK；未接纳的容量/治理拒绝没有 ACK 义务。
- 本机 BudgetError 不透传预算元数据。远端 ResourceExhausted 不证明对方字节配额不足，收到 reset 也不证明用户主动取消。客户端接收额度不足用例中，本机保留 BudgetError，服务端仍保留 LostTerminals，不伪造 CancelRequests。
- CancelRequests 是首次收到有效 CANCEL 的观察，不证明用户动机或最终胜出；业务返回 Canceled/DeadlineExceeded 不增记这个观察。旧 Canceled 为兼容保留，不能当作主动取消总数。
- 已结束回执满足 `ReceiptsResolved = Delivered + LostTerminals + ReceiptsCanceled + ReceiptsDeadlineExceeded`；等待中的请求尚未结算。ACK 已收而 QUIC 客户端 FIN 未确认继续记 IncompleteExchanges。
- DrainWithReport 的 PriorFaults / AddedFaults / TotalFaults 区分历史、本次新增和累计；Quiescent 区分资源退出。资源静止可与 ErrDelivery 同时成立。后续成功调用、重复或并发 drain 不清空历史。忽略取消的 handler 不会被假装已终止；它未退出时报告仍保留 InFlight/Executing。
- ACK 是协议层终态回执，不是应用消费、持久化或业务提交。内存快照和请求统计各自受锁保护，报告不声称全域原子快照；未静止时返回之后仍可新增累计事件。

可运行的正式接入示例：[SDK 示例](../../../drain_example_test.go)，配置/计数说明：[契约](../../drain-r9c.md)。

## 测试与失败保留

最终源码的 Go 1.27.1 普通/race、最低 Go 1.26.0 普通/race 四轮，各 **332 PASS / 0 FAIL / 0 SKIP**；每轮创建并验证自己的真实 etcd 3.7.1 单成员，结束后回收自己创建的进程：[本机普通](final-local/result.json)、[本机 race](final-race/result.json)、[最低版本普通](final-minimum/result.json)、[最低版本 race](final-minimum-race/result.json)。[清理收据](etcd-cleanup.json)只删除已确认回收进程的本轮独立 etcd 数据目录，没有触及共享服务或历史证据。

新增边界用例 **460 次 race 检查通过**：[完整输出](final-contracts-race.jsonl)。覆盖三传输 ACK/CANCEL/deadline/无 ACK/reset/缺 FIN、服务端发送预算与业务同名 Status、重复/排队取消、接纳拒绝、读取/控制预算中断、后续成功及重复/并发 drain、未合作 handler 和客户端接收预算。计数包含子用例，不是 460 个独立场景。

终态事件模型 fuzz 最终普通运行 **383,088** 次、race **14,610** 次通过：[最终普通](final-fuzz.txt)（此前候选 [326,210 次](fuzz.txt) 另行保留）、[race](fuzz-race.txt)。模型覆盖 ACK/错误 ACK/取消/通道中断/重复结算的顺序，属于有限事件模型验证，不是任意网络协议证明。[独立模块 SDK](final-sdk/result.json)使用 GOWORK=off，在外部模块编译运行公开 API 的真实 TCP 示例与新 drain 示例，race 各重复 3 次；这是本机 replace 接入，未宣称已发布正式版本。[build/vet](final-static-checks.json)通过。

首次新夹具测试中，有预算先触发层级的预期错误及 TLS 未设置 ALPN 的错误；保留 [10 个失败（含父用例）](contracts-initial.jsonl)，修正夹具后 [185 次 race 检查通过](contracts-race.jsonl)。控制预算夹具的无效 1024B 配置也保留在 [初次输出](control-cause-before.jsonl)，未将这些夹具错误计为框架故障。

## 有限资源与内部基线

资源对照使用 R9B `0b33dad` 与冻结的最终源码：TCP/QUIC，各版本宽预算 60 秒、饱和 30 秒，每组 6 个负载/退休周期，正式对照 **360 秒活动负载、48 个周期**。最终版的[源码 hash](final-source-manifest.json)与[最终资源快照](final-resources/metadata.json)逐文件匹配；R9B QUIC 适配器、memory_pool.go 与 routing/client.go 保持逐字节相同。两版本注入同一实验 driver，不做 gRPC 对照。资源观察允许共享主机上的并行正确性检查，不据此解释吞吐、容量或错误率改善。

| 版本 / 传输 / 额度 | offered | success / canceled | 其他失败 | LostTerminals | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| R9B / TCP / 宽 | 787,617 | 590,714 / 196,903 | 0 | 0 | PASS |
| 最终 / TCP / 宽 | 399,060 | 299,295 / 99,765 | 0 | 0 | PASS |
| R9B / QUIC / 宽 | 211,647 | 158,736 / 52,911 | 0 | 0 | PASS |
| 最终 / QUIC / 宽 | 175,771 | 131,828 / 43,943 | 0 | 0 | PASS |
| R9B / TCP / 饱和 | 526,960 | 27,533 / 15,220 | 484,207 | 96,740 | DEGRADED |
| 最终 / TCP / 饱和 | 590,213 | 29,862 / 15,739 | 544,612 | 109,668 | DEGRADED |
| R9B / QUIC / 饱和 | 89,071 | 9,986 / 9,680 | 69,405 | 12,941 | DEGRADED |
| 最终 / QUIC / 饱和 | 131,287 | 14,422 / 13,901 | 102,964 | 19,961 | DEGRADED |

[可读对照与每类错误](resource-comparison.json)、[R9B/早期候选完整记录](resources/summary.json)、[最终版完整记录](final-resources/summary.json)。所有组均回收自己创建的 worker/client；最终池账户/字节归零、周期内峰值不越界、退休时未读数据仍计费。空闲 FD 在宽预算 TCP/QUIC 与饱和 QUIC 均为 9、饱和 TCP 为 8；对应 goroutine 分别为 7/10/10/6，6 个周期内未增长。进程保留正常 runtime 对象，不能解释为整个进程没有 goroutine、整个 RSS 有硬上限或长期无泄漏。

最终版宽预算的 CancelRequests 精确观察到 TCP 99,765、QUIC 43,943 个 CANCEL；没有未回执。饱和下本机接收拒绝可能关闭 TCP 上其他调用，或者 reset 单个 QUIC 交换。最终版分别保留 TransportAborts 475,482 / 102,970 与 LostTerminals 109,668 / 19,961；这些不是相同分母、也不一一对应，通道中断可能发生在尚未产生终态之前。没有收到 CANCEL 的中断不改记为主动取消。客户端 success 与服务端 Delivered 也可能不同：客户端收到 END 不等于服务端最终收到 ACK。

每个最终 worker 都保存 DrainWithReport，并断言 Quiescent 与累计故障一致。所有饱和组继续以 ErrDelivery 退出；没有通过清空计数使 drain 变绿。早期候选还完成了 180 秒活动负载、24 个周期，完整保留在 resources；最终控制错误原因修复后补跑 4 组，不拿早期候选的结果充当最终源码结果。

吞吐对照在所有上述检查结束之后独立串行执行，只比较 R9B 与当前版本。


串行吞吐验收：R9B/最终版配对 worker/client，TCP/QUIC，普通/16 chunk 流式，128B/16KiB，managed 两连接，8 个工作协程，5 个种子、每组 1000 次，64 次预热不计入计时；共 **80 case / 80,000 offered，全部成功**。无重复执行或未回收进程，每组池账户/字节归零且峰值不越限。

| 传输 / 调用 / payload | R9B req/s | 最终 req/s | 中位变化 | p95 ms（旧→新） | 三进程 CPU s（旧→新） |
| --- | ---: | ---: | ---: | ---: | ---: |
| TCP / unary / 128B | 55,598 | 53,286 | -4.16% | 0.252→0.262 | 0.103→0.106 |
| TCP / unary / 16384B | 22,052 | 20,902 | -5.21% | 0.617→0.671 | 0.216→0.225 |
| TCP / stream / 128B | 13,093 | 13,075 | -0.14% | 0.939→0.949 | 0.403→0.404 |
| TCP / stream / 16384B | 3,526 | 3,604 | +2.22% | 3.326→3.228 | 1.274→1.284 |
| QUIC / unary / 128B | 26,849 | 25,219 | -6.07% | 0.471→0.501 | 0.195→0.203 |
| QUIC / unary / 16384B | 6,534 | 6,779 | +3.74% | 1.764→1.643 | 0.758→0.746 |
| QUIC / stream / 128B | 13,898 | 13,427 | -3.38% | 0.868→0.897 | 0.366→0.370 |
| QUIC / stream / 16384B | 1,045 | 1,040 | -0.56% | 9.909→10.262 | 4.869→4.926 |

[本轮同环境对照](performance-comparison.json)、[完整 5 次样本/最小最大/状态](performance/summary.json)、[版本/源码/二进制与 driver hash](performance/metadata.json)。128B QUIC 普通调用 −6.07%、小流式 −3.38%，TCP 普通 128B/16KiB −4.16%/−5.21% 等负中位数全部保留；也有大包普通调用或 TCP 大包流式的正变化。新增回执互斥与累计计数仍有成本，但这些短跑样本不能把主机波动与代码开销精确分离，不宣称正变化为优化收益，也不以资源实验 offered 数量推算吞吐。

p95/p99 只针对成功请求，三进程 CPU 包含启动/预热/关闭；本机固定发现快照和 JSON payload 是 fixture，不是双机、实际推理或生产容量证据。R9B 小帧发送恢复机制仍保留，未通过删除预算或退休统计提高数字。

80 组逐次 offered、worker 和日志无损保存于 [原始归档](performance/raw-cases.tar.gz)，根目录 [results 索引](performance/results.json)指明每组 client.json 成员；[打包收据](performance/packaging-receipt.json)逐成员校验 SHA-256 后才删除本轮松散 case 目录。首次打包误将已有薄索引当作包含 offers 的完整结果，触发 KeyError；该次没有删除目录或丢失数据，收据保留失败，随后按每组原始文件重新验证完成归档。

已有网络/重置用例额外完成 **30 次 race 检查**：[UDP relay 黑洞、慢消费、取消恢复与池退休](network-reset-race.jsonl)。这里是本机 UDP relay 故障，未新增宿主机 qdisc 修改或真实双机实验。

## 尚未解决的设计风险与下一步

1. **真实双机和长时混合负载验收。** 本轮 loopback、进程、真实单成员 etcd、UDP relay 故障不代表真实 WAN/NIC/MTU、etcd 多成员或小时/天级稳定性；vLLM/GPU 未验证。下一步优先做两台 Linux 主机上的丢包、慢消费、滚动 drain 与长时资源账本对齐。
2. **饱和时的交付与业务幂等边界。** 配额拒绝可能让记录连接或 QUIC 交换中断，终态仍可能未确认。本轮仅澄清原因和保留失败；没有自动重放、exactly-once 或业务提交保证。需要应用侧幂等/恢复契约后，才能讨论安全重试；不能仅凭 reset 判断“服务端没执行”。
3. **有界发送及资源治理成本。** 保留固定 quic-go 版本的安全回退、共享预算和退休统计；框架额度不约束用户 codec 任意分配、QUIC 内部或整个 RSS。后续应按 profile 优化 packetization/大包复制与锁争用，并重复 reset 与预算回归，不能删除安全机制换取数字。
