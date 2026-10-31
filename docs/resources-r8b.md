# R8B：编码前资源接纳与有限长期验证

当前请求先 JSON 编码，再等待 writer/检查在途和反馈；QUIC 先分配原生流。先保存 R8A 源码并用阻塞/计数编码器复现，再逐步实现：

| 阶段 | 实现 | 验证 |
| --- | --- | --- |
| B1 | 公共客户端共享编码前接纳票据，计入编码、等 writer、在途及收尾；压力拒绝也在编码前 | MaxInFlight=1 的 16 个阻塞编码尝试；已关闭/预取消/压力拒绝不运行用户编码器；QUIC 子流共享 |
| B2 | 编码容量预留、编码结果大小与正文/OPEN 复制计费；避免无限保留等待 writer 的编码结果 | 超大输出、慢 writer、编码失败/panic、部分发送、取消与关闭；对照计费峰值 |
| B3 | 可观察的 Preparing/Encoding/Active 与 WaitIdle | Close 封闭新接纳并回收 IO，但不强杀正在调用的用户 MarshalJSON；释放后名额/字节归零，不创建脱离调用者的编码 goroutine |
| B4 | 独立进程资源 soak、goroutine/heap/GC 曲线及负载对照 | warmup、多个压力/清理阶段、旧基线与最终版、普通/race/fuzz、etcd 与流式回归；保留保守预留导致的拒绝/性能退化 |

预算约束框架的有效编码结果及记录复制，不是 Go allocator 或 RSS 上限。任意 MarshalJSON/JSON 编码内部可分配、阻塞、panic；同步编码不保证在配置 timeout 时立即返回，编码返回后重新检查取消/关闭且不发送过期业务。资源 soak 是明确时长的有限观察，不能写成无限期无泄漏。

## 当前 API 与范围

`MaxInFlight` 现在在调用用户编码器之前检查，覆盖 Preparing、等待 writer、pending 和实际交换收尾。QUIC 的所有子流共享公共客户端的名额，接纳和编码先于原生业务流分配。方法压力反馈也在编码前接纳；非法方法、预取消、已知关闭连接不调用编码器。

`MemoryConfig.EncodingBytes` 是每个公共客户端的编码容量，默认 `max(16 MiB, 2*MaxFrameBytes)`，显式配置必须至少能容纳两帧。编码前先预留两份最大帧容量，覆盖有效请求 JSON 和 OPEN 正文复制；编码完成后缩减为实际正文与元数据的保守容量，等 writer 时仍计费，写入收尾后释放。合并发送包继续单独使用 SendBytes。编码容量不足时在 MaxInFlight 限定的调用者中等待，可被用户预算或客户端关闭取消；未取得容量不进入编码器。

初稿曾在编码容量不足时立即拒绝，普通小请求突发发生回归，原失败日志保留。最终改为有界等待；最坏情况容量预留仍可能降低并行编码数量、增加延迟。它不是任意 JSON 编码内部内存的硬上限，超大/自定义编码器在返回前仍可自行分配，超大结果返回后拒绝且不发送。固定上限与两个复制的预留不能写成零拷贝或全进程内存保证。

`AdmissionStats()` 返回 Limit/Active/Preparing/Encoding/EncodingWaiters/Peak/Rejected/Closed。静态在途容量不足会快速 ResourceExhausted，编码容量等待不另起编码 goroutine。名额引用覆盖准备函数、pending 和 QUIC 原生交换收尾，避免取消先返回就提前释放实际仍在用的资源。

`Close()` 封闭新接纳、取消等待和 IO、回收已结束未读缓冲；它仍不能终止正在同步执行的用户 MarshalJSON。`WaitIdle(ctx)` 等待已接纳工作、编码和交换收尾，先 Close 再等待可观察残留编码。它不等待空闲常驻 reader，也不代表应用已消费未关闭客户端持有的全部流；未读结果由 MemoryStats 单独计费。用户编码器 panic 继续传播给调用者，但资源用 defer 释放；框架不创建脱离调用者的编码任务来假装超时。

显式连接失败先发布 pending 的终态原因，再取消 owner Context，防止 ErrClosed 被次生 context.Canceled 覆盖；编码/写许可等待也保留关闭原因。QUIC 父连接结束会封闭共享接纳并取消准备上下文，后续请求不再白做编码。

## 验证入口

```sh
python3 scripts/verify-discovery.py --output /tmp/kanarpc-r8b-race --race
go test . -run='^$' -fuzz='^FuzzPreparedOpen$' -fuzztime=15s -parallel=2
python3 scripts/verify-resources.py --output /tmp/kanarpc-r8b-resource --duration 180 --cycles 12
python3 scripts/verify-resources.py --output /tmp/kanarpc-r8b-performance --no-soak
```

资源对照从实际 R8A 根提交构建基线，在两个版本上注入同一 resource-lab 程序；先冻结源码，再启动独立 worker/client。上面命令让 TCP/QUIC 各对旧/新运行 180 秒活动窗口、12 个负载/清理周期，共 720 秒活动窗口，另有预热和清理时间。负载混合普通、完整流、取消流和已完成未读流，记录强制 GC 后的 heap/goroutine/GC 及全部计费尾部；不保留随请求总数增长的业务 ID map。早期观察器把采样历史留在测量进程中，已经改为逐样本/逐阶段写 JSONL，Python 在进程退出后才读回，原受干扰曲线保留。默认脚本仍是 60s/6 轮，长时长须显式指定；分钟级样本不代表小时/天级生产验证。

随后用同一 transport-lab 程序做交替版本的普通/流式完整 offered 对照；`--no-soak` 只跑该性能部分。最终性能对照在其他验证结束后串行执行，race 计时不作性能证据。详细数据、后续 QUIC 错误映射回归和负结论见 [验收报告](verification/resources-r8b/report.md)。

后续 R8C 已接入发现快照更新、连接建立去重、有界连接数、旧 endpoint 退休与未读流保护，见 [连接管理](connections-r8c.md)；不自动重放业务。CampusTrace 不在本轮修改范围。
