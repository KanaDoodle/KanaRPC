# R9B：QUIC 发送缓冲规避成本与优化

2026-10-31，以已推送的 `478ab3d` 为起点，仅修改 KanaRPC。[先行步骤/API](../../quic-r9b.md)。CampusTrace 未修改；当前验证为 Mac loopback、独占单成员 etcd、固定 JSON/发现 fixture，不是真实双机、WAN/NIC、GPU/vLLM 或生产容量实测。

## 问题定位与对照

R9A 为规避固定 quic-go v0.64.1 在并发 reset 后重新缓存发送尾部、最终耗尽原生流额度的问题，将 nativeChannel 写入统一改成 `WriteWithLimit(identity)`。该非空 limiter 关闭小帧缓冲，DATA/CREDIT 每次写入需要等 packetizer 处理。代码路径本身和性能代价需要分别验证，不能直接恢复 nil limiter。

在隔离副本中仅改这一个发送表达式，保留相同预算、连接和退休实现；新增相同的真实 QUIC benchmark：8 个调用者、每请求 16 块、窗口 8，128B 字符串，Recv(nil) 跳过应用解码。三个独立短重复的初始对照：

| 写入方式 | 中位 ns/RPC | 包/RPC | wire B/RPC | 分配 B/RPC |
| --- | ---: | ---: | ---: | ---: |
| R9A identity | 173,834 | 28.85 | 3,891 | 65,659 |
| 旧 Write，仅诊断 | 65,648 | 6.538 | 3,125 | 44,243 |

这是受控局部消融，显示发送等待/包数与分配增加；不是完整应用性能。独立 CPU/block profile 中 identity 写路径出现 packetizer 等待，CPU 主要落在 UDP 发送/syscall。profile 有额外采样成本，不能把 profile 的耗时/累计 goroutine block 秒数直接当性能指标。[初始对照](limiter-benchmark.txt)、[旧写](unsafe-write-benchmark.txt)、[CPU](limiter-cpu-top.txt)、[block](limiter-block-top.txt)、[原始 profile](profiles-initial.tar.gz)。

旧 Write 本轮 3 次饱和恢复检查全通过：[负对照](unsafe-reset-control.jsonl)。保留这个有限负结果，不能据此推翻 R9A 捕获的 64 条发送流未退休证据或声称旧写法已安全。该版本只作性能参照，不发布。

## 失败候选、额外开销与最终实现

1. 盲目 TryWriteAll 候选在 `SetWriteDeadline` 和 `SetDeadline` 两种过期条件下都接受了不该发送的字节，测试失败：[原始反例](naive-deadline-before.jsonl)。根因是该公开 API 明确忽略写 deadline。R9A 原路径通过同组契约测试：[原契约 race](deadline-contract-r9a.jsonl)。
2. 加 deadline guard 后，每次仅允许 ≤1024 的原子写和任意大小原子写候选都通过基本 race 契约；但前者不能限制多次写累计排队，后者的大包分片反复复制成本显著。[候选源码](diagnostic-source-manifest.json)、[两种完整 payload 结果](local-spectrum-summary.json)。
3. 16 KiB payload 的局部 benchmark，任意大小原子写约 1.81ms、4.94MB/RPC 分配；每次小写限制版约 1.20ms、1.06MB/RPC。4 KiB payload 分配也从约 0.31MB 增至 1.41MB。quic-go 原子大帧后续分片复制剩余部分，不能只挑 128B 的正结果。因此这两个候选均未采用。
4. 最终每条 native stream 采用 **4 KiB 保守原子批量额度**。符合 deadline 和剩余额度时使用 TryWriteAll；ErrWouldBlock 零入队后才回退到原 identity 路径。非空 paced write 成功处理有序前缀后才补回额度，空写、清除 deadline、失败不制造额度。deadline mutex 只保护检查/原子接纳，阻塞写前释放，取消仍可设置即时 deadline。

选定候选的局部 128B 中位约 70.9µs、6.54 包/RPC，接近旧写法的包数；900B 分配约 122KB/RPC，低于未约束累计队列候选的约 191KB。4 KiB 和 16 KiB 不强制原子复制，保留 paced 路径及其成本。不同 payload 的完整正负数据不合并取最好值。

生产修改集中于 nativeChannel，未修改协议、依赖版本、编码/接收/发送预算账本或 routing 退休逻辑。link 仍先 reserveWrite，写完/失败后释放；原子接纳不绕过池级额度。QUIC 内部缓冲和重传一直不在框架 RSS 承诺内，4 KiB 额度也不是实际未发送/未 ACK 字节的观测值。record writer 由 link permit 串行化；并发 deadline 更新仍受支持。

## 正确性、race、fuzz 与资源

Go 1.27.1、最低 Go 1.26.0，普通与 race 四轮完整测试各 **278 PASS / 0 FAIL / 0 SKIP**。每轮 runner 创建并验证自己的 etcd 3.7.1 单成员，回收自己的进程：[普通](go-local/result.json)、[race](go-race/result.json)、[最低版本普通](go-minimum/result.json)、[最低版本 race](go-minimum-race/result.json)。历史 R9A 的 266 项未冒充当前结果。

新增实际 QUIC 原生流测试覆盖过期 deadline 不入队/不达 peer、清除期限复用、填满 64 KiB 流控窗口后的回退与零重复、回退期间取消并归还共享发送租约、并发期限更新、有界批量与空写不补额度。独立 race 10 轮共 110 项通过：[契约回归](native-write-after-race.jsonl)。

保留原 16 KiB 饱和 reset 恢复，增加 900B/1024-byte 接收池的原子小记录 reset 场景，均要求实际发生接收拒绝、超过 128 次 native open，再验证同 session 恢复；连同退休/黑洞等聚焦 race 三轮 27 项通过：[池/reset 回归](reset-pool-after-race.jsonl)。没有以换 session、重试业务或扩大超时掩盖卡死。

FuzzNativeQueuedWrites 使用真实 QUIC 流，混合空/小/大写、过期/未来 deadline、paced 回退、最终 FIN 与逐字节结果比较，并检查保守额度边界。普通约 31 秒 **49,762 次**，race 约 33 秒 **6,344 次**，均通过；race 后段计数停滞的原始输出仍保留，不宣称固定 fuzz 吞吐或完备性：[普通](fuzz-native-queue.txt)、[race](fuzz-native-queue-race.txt)。独立公开 SDK 的 race 三轮通过：[SDK](sdk/result.json)。build/vet/Python 编译均通过：[检查收据](checked-build-vet.json)。

R9A 与最终版本的预算、未读/退休周期、goroutine/FD 对照及最终冻结源码后的串行多 payload 矩阵均已完成，详见下节。

## 最终性能与资源对照

### 资源与饱和行为

[R9A/当前资源矩阵](resources/summary.json)使用相同 driver、各版本配对的真实 worker/client。每版宽预算 120 秒、饱和 60 秒，各 12 周期；合计 **360 秒活动负载、48 个周期**。它是有限分钟级观察，并与部分正确性检查共享主机，不作吞吐收益解释。

| 版本/场景 | mixed offered | success / canceled | resource_exhausted | 周期 FD / goroutine | 结果 |
| --- | ---: | ---: | ---: | ---: | --- |
| 当前/宽预算 | 483,966 | 362,973 / 120,993 | 0 | [9] / [10] | PASS |
| R9A/宽预算 | 537,299 | 402,973 / 134,326 | 0 | [9] / [10] | PASS |
| 当前/饱和 | 343,184 | 35,962 / 34,693 | 272,529 | [9] / [10] | DEGRADED |
| R9A/饱和 | 333,577 | 31,680 / 29,503 | 272,394 | [9] / [10] | DEGRADED |

四组每周期 Total.Used 与退休账户归零、峰值不越池额；父 Close/Wait 后累计 Created=Released、Accounts=0，server 实时资源也归零。退休期间的约 16 KiB 未读结果仍计费，直到消费/关闭才释放。宽预算两组 PASS，饱和两组保留为 DEGRADED：拒绝及 LostTerminals 使服务端 drain 返回错误，不能把“字节回收”当成“全部业务成功”。两版本的具体丢失终态计数和账户数见 [资源摘要](resource-observations.json)。本轮没有再出现 native stream 耗尽后的恢复卡死，但不证明所有底层库资源/RSS 或小时/天级负载无泄漏。

### 串行进程性能

最终 **600 case / 600,000 offered 全成功**：三个运行时版本 × TCP/QUIC × 普通/16 块流式 × 两连接直接复用/两连接管理池 × 五种 payload × 五种子。每 case 1,000 个计时 offered、64 个预热、8 个 closed-loop 调用者，GOMAXPROCS=2；各版本位置按种子轮换。发现是固定 snapshot fixture，正常 JSON 解码/校验。SDK、fuzz、资源和本轮正确性任务均在正式对照前结束；共享主机外部背景负载仍未严格隔离。

每个版本注入相同实验 driver；源码、二进制 hash 和 payload 参数保存，[元数据](final-comparison/metadata.json)还断言 resources.go、memory_pool.go、routing/client.go 与 R9A 逐字节相同，未删除资源治理换取吞吐。managed 每个 case 均检查最终池账户/字节为零、峰值不越限。无重复执行、残留调用或未回收进程。[完整组摘要/五个重复与范围](final-comparison/summary.json)、[所有 case 索引](final-comparison/results.json)。原始 offered 和日志逐成员校验 SHA-256 后无损归档：[全部原始 case](final-comparison/raw-cases.tar.gz)、[打包收据](packaging-receipt.json)。可将 raw-cases.tar.gz 解到空目录查看每次调用；薄索引不丢弃失败，完整五次样本/范围继续在摘要中可读。

QUIC 管理池流式的五次中位数：

| payload B | R8D req/s | R9A req/s | 当前 req/s | 当前相对 R9A | 当前相对 R8D | p95 ms R9A→当前 | triple CPU s R9A→当前 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 128 | 13,327 | 6,555 | 13,130 | +100.3% | -1.5% | 1.720→0.900 | 0.820→0.370 |
| 900 | 7,323 | 5,281 | 7,890 | +49.4% | +7.7% | 2.176→1.492 | 0.974→0.650 |
| 1024 | 7,421 | 5,842 | 7,454 | +27.6% | +0.4% | 1.933→1.552 | 0.900→0.681 |
| 4096 | 3,370 | 3,093 | 3,308 | +6.9% | -1.8% | 3.481→3.302 | 1.660→1.551 |
| 16384 | 1,022 | 1,044 | 1,053 | +0.8% | +3.0% | 10.051→9.764 | 4.992→4.941 |

原 128B 场景本轮 R9A 相对 R8D 约 −50.8%，与上轮约 −53% 的退化方向一致。当前 **6,555→13,130 req/s（+100.3%）**，接近本轮 R8D 的 13,327（仍低约 1.5%）；p95 **1.72→0.90ms**、三进程 CPU **0.820→0.370s**。当前直接复用约 14,169 req/s，管理池仍比直接复用低约 7.3%，该管理成本未隐藏。

这只说明小记录本地场景基本恢复，不能宣称所有 payload 翻倍。16 KiB 流式相对 R9A 仅约 +0.8%；直接复用 16 KiB 相对 R8D 仍约 −1.0%，4 KiB 管理池相对 R8D 仍约 −1.8%。相对 R9A 还保留 **QUIC 普通管理池 16 KiB −1.38%**、TCP 直接流式 1024B −5.65%、TCP 普通直接 1024B −4.21% 等中位负变化：[全部负中位数组](negative-medians.json)。TCP 运行时路径未改，正负波动不能归功于 QUIC 优化。

CPU 是三个独立进程的整个生命周期（含启动/预热/关闭）；p95/p99 仅成功请求。五个短重复仍不足以证明生产容量、尾延迟保证或通用收益，不把 fixture 数值写成真实推理性能。大包 paced 发送和现有池/退休开销均保留。

## 限制与下一步

- 真实双机、真实网络硬件/MTU/WAN、小时/天级压力、多成员 etcd、真实模型推理未验证；所有吞吐都是本地 JSON fixture。
- 4 KiB 是保守批量额度，可能提前触发 paced 写入，长流/大包不会获得与小请求相同的收益；deadline 锁、资源账本和退休统计仍有成本。框架预算不约束用户 codec 任意分配、所有管理对象或 RSS。
- 饱和拒绝、终态回执丢失及 sticky drain 错误需要原样保留，资源归零不等于业务全部成功；协议 ACK 也不是数据库提交。
- 下一步优先：带 reset 回归的上游发送修复/更有效的有界 packetization；真实双机和长时混合负载验证；基于 profile 再优化大包复制与池锁，不恢复已知危险写法或为好看的吞吐删掉预算和统计。
