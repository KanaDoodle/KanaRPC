# QUIC 有界批量发送与安全回退（R9B）

R9A `478ab3d` 采用安全写路径规避已复现的 reset 退休问题，流式吞吐随之下降。R9B 通过有界原子入队降低小记录的发送等待，同时保留 reset、预算和退休语义。

| 阶段 | 结合当前代码的步骤 | 验收 |
| --- | --- | --- |
| B1 定位 | 冻结 R9A，保留编码/收发共享预算与退休实现；用相同流式 benchmark 对比 identity limiter 和仅供诊断的旧 Write，采样 CPU/block/包数 | 实际复现退化，记录每次 RPC 包数、分配、阻塞栈；旧 Write 必须重跑 reset 压力负对照，不能仅凭吞吐恢复就采用 |
| B2 候选 | 用公开原子入队 API 避免小写每次等待 packetizer；保留安全的阻塞回退；根据局部实验选择有界队列策略 | TryWriteAll 忽略 deadline，候选必须主动遵守过期、更新、清除和取消；WouldBlock 保证零入队后才允许原有回退，无业务重放；reset/Close 不恢复旧脆弱分支 |
| B3 正确性 | 先设计边界测试，再改 nativeChannel 适配；不重构协议/路由/资源账本 | 真实 QUIC deadline/流控不足/partial write/reset/并发 deadline、池饱和和退休、网络黑洞；普通/race、最低 Go、isolated etcd、fuzz；保留失败版本 |
| B4 对照 | 冻结候选后串行测 R8D、R9A、新版；直接复用和管理池，128B 到大 payload，按版本交替运行 | 每次 offered/失败/延迟/CPU/源码 hash 全保留，既比上轮慢版，也比修复前强基线；不取最好样本，不覆盖旧报告；有限周期资源复验 |

预算保护点仍是 link.writeLocked 的 reserveWrite 与 wire.Write/Release，禁止绕过租约提前写入。QUIC 内部复制、socket/传输缓冲一直不在框架字节预算/RSS 承诺内；候选若增加底层排队，必须解释边界并做资源验证。

## 已采用的发送方式

`nativeChannel` 先检查缓存的写 deadline 与每流 4 KiB 的保守批量额度，满足时调用公开的 `TryWriteAll`。它与 quic-go 的 send mutex 一起完成原子检查/复制，不进入旧的 Write 尾部重缓冲路径。额度按成功入队的字节累计；不会因为应用清除 deadline、空写或一次失败而补回。

额度不足、deadline 已过或 QUIC 返回 ErrWouldBlock 时，使用 R9A 的 `WriteWithLimit(identity)`。ErrWouldBlock 明确保证零入队，所以回退不会重复发送业务字节。只有一次非空 paced write 成功完成，才把保守额度归零：有序前缀也已交给 packetizer。deadline mutex 在所有阻塞 IO 前释放，允许取消更新期限。SetWriteDeadline/SetDeadline 的更新与原子入队检查按同一把锁排序；record writer 仍由 link 的 permit 串行化。

这个额度限制每条 native stream 在两次 paced write 之间的额外原子入队，消息与流仍可超过这一大小；QUIC 内部拆分、重传及内存归属见 [使用约束](operating-notes.md)。现有 reserveWrite、发送租约、池聚合和退休引用的代码未改。

局部实验还保留了两个未采用版本：仅限制每次 Write 大小不能限制多次累计入队；所有大小都原子入队则在大包分片中造成大量额外复制。最终采用有界批量与安全回退，nil limiter 的已知 reset 风险作为负对照保留。

完整验证入口：

```sh
python3 scripts/verify-discovery.py --output /tmp/r9b-normal
python3 scripts/verify-discovery.py --race --output /tmp/r9b-race
GOMAXPROCS=4 go test -race . -run '^$' -fuzz '^FuzzNativeQueuedWrites$' -fuzztime=30s -parallel=4
python3 scripts/verify-pool.py --baseline 478ab3d --output /tmp/r9b-resources --cases quic-roomy quic-tight --duration 120 --cycles 12 --tight-duration 60
python3 scripts/compare-quic-write.py --output /tmp/r9b-performance
```

性能 runner 向各历史版本注入同一份实验 driver，只增加 payload 大小与验收统计；每个版本都运行自己的真实 worker/client。默认包含 R8D（已知 reset 风险的强性能参照）、R9A（正确但慢）、当前版本，TCP/QUIC、普通/16 块流式、直接复用/管理池、五种 payload、五种子。R8D 用作发送成本参照，其已知 reset 风险单独保留。原始 offered/失败/资源数据和源码 hash 保留；历史报告不覆盖。
