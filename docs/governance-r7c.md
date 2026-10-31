# 方法调度与自适应过载控制（R7C）

通过共享字节预算、有界方法调度、延迟窗口 AIMD 与短效反馈，将压力控制同时接入服务端执行和客户端发送前接纳。TCP/TLS/QUIC 共用取消、回执与关闭契约。下表记录 R7C 的实施和验证路径。

| 阶段 | 实施内容 | 对照与验证 |
| --- | --- | --- |
| C1 | 全局接收/发送字节租约，正文分配前预留，独立控制字节池，QUIC 所有子流共享，已完成未读流继续计费 | 小配置复现多头声明的预算前分配，部分帧/取消/迟到数据/完成未读/关闭回收；Peak 不越限；预算不是 RSS |
| C2 | 在途、排队、执行、终态等待分开；有限队列和方法权重；队列超时/取消、deadline drain | 服务端配置方法权重；work-conserving FIFO 强基线；满载控制和无执行的排队取消；不是租户或 token 公平 |
| C3 | 方法延迟 AIMD，短效 epoch/sequence 压力元数据，客户端发送前可选接纳 | 固定高并发/FIFO、固定高并发/公平、固定低并发/公平、自适应、反馈辅助五组；反乱序、失效、空闲连接不分摊需求；不做业务重试 |
| C4 | 串行独立进程负载、故障复现、最低 Go/当前 Go 普通与 race、fuzz、R7B/etcd 回归 | 使用 JSON/定时器负载，保存 offered、拒绝、退化和资源尾部 |

记录预算覆盖入站正文、出站合并包和 Sender 等信用时保留的正文，编码预留由 R8B 扩展。流通过消费或 Close 释放缓冲，公共父客户端 Close 回收其未读部分。完整计费范围见 [使用约束](operating-notes.md)。

执行采样从真正开始处理器到处理器返回，排队和终态 ACK/FIN 另记；流处理器中的信用等待仍属于执行占用。关闭期间保留未返回处理器统计，不能强制杀 Go 函数。方法权重来自服务端配置，调用方不能自报权重。方法权重由服务端配置，身份与租户成本管理见 [使用约束](operating-notes.md)。

控制思路参考 [Netflix concurrency-limits](https://github.com/Netflix/concurrency-limits) 与 [Envoy adaptive concurrency](https://www.envoyproxy.io/docs/envoy/latest/configuration/http/http_filters/adaptive_concurrency_filter.html)。本实现采用窗口 p90 与 AIMD，并提供显式方法权重；下文说明具体控制规则。

## 显式启用

```go
server, err := kanarpc.NewServer(kanarpc.ServerConfig{
    MaxInFlight: 128,
    MaxPerConnection: 64,
    Memory: kanarpc.MemoryConfig{
        ReceiveBytes: 16 << 20, SendBytes: 16 << 20, ControlBytes: 64 << 10,
    },
    Governance: &kanarpc.GovernanceConfig{
        MaxExecuting: 16, QueueCapacity: 64,
        QueueTimeout: 60*time.Millisecond,
        QueuePolicy: kanarpc.MethodFair,
        Adaptive: true, Feedback: true,
        SampleWindow: 16, FeedbackTTL: 500*time.Millisecond,
        Methods: []kanarpc.MethodPolicy{
            {Method: "/demo/analyze", Weight: 1, Min: 2, Initial: 8, Max: 16, Target: 20*time.Millisecond},
            {Method: "/demo/status", Weight: 2, Min: 1, Initial: 4, Max: 4, Target: 5*time.Millisecond},
        },
    },
})
// Check err, register BOTH methods, then Serve / ServeTLS / ServeQUIC.
// A configured method that was not registered fails Serve before ownership.

cfg := kanarpc.ClientConfig{Feedback: &kanarpc.FeedbackConfig{
    MaxAge: time.Second, MaxMethods: 128,
}}
// Pass cfg to Dial, DialTLS, DialQUIC, or DialOptions.Client.
```

将该配置与处理器注册、监听和 TLS 信任配置组合，并逐步检查返回错误。`Governance=nil` 保留无队列的静态接纳；`Feedback=nil` 不开启客户端反馈。可使用 `ObserveOnly:true` 记录建议而不执行本地拒绝。发现驱动连接管理见 [routing](connections-r8c.md)。

## 字节与生命周期

每个 Server 的所有记录连接/原生流共用接收、发送预算；每个公共客户端共用自己的预算，QUIC 子流不能各领一份。不同 Dial 对象不共享进程级总预算。默认主接收/发送各 16 MiB，控制接收/发送各 64 KiB；`ControlBytes` 分别用于两个方向，不是两方向合计。控制 payload 限 4096 字节；主预算至少 1 KiB、控制预算至少 4 KiB，越界配置报错。

读取先验证头，再非阻塞预留，之后才分配正文。OPEN 按正文两倍保守覆盖记录与 JSON 请求拷贝；DATA 在 Recv 解码结束后释放。写入先预留正文+24 字节头的临时合并包；服务端 Sender 在等信用期间还保留并计费原始正文。所以最大帧不保证可在任意更小预算下发送，需要为并发和复制留空间。预算拒绝不排队等内存。

正常 END 之后尚未消费的 DATA 仍计费，直到消费、Stream.Close 或公共父客户端 Close。QUIC 子流的自动 IO 收尾不能提前释放这部分计费。取消、读错误、迟到帧、坏帧和父关闭都走幂等租约释放；SESSION_BARRIER 同样使用预算。没有业务 OPEN/方法信息可用时，预算拒绝可能关闭 TCP 记录连接或 reset 单个 QUIC 交换，不能保证每次都收到结构化 ResourceExhausted。

`MemoryStats()` 的 Used/Peak/Rejected 是框架租约统计，不是 heap/RSS。[编码前接纳](resources-r8b.md) 将名额与编码预留提前到用户编码器调用之前；[池级预算](pool-r9a.md) 进一步聚合多连接的编码、接收与发送额度。用户处理器可保存解码对象、编码器可自行分配，TLS/QUIC 内部和 Go allocator 不受这个预算限制。控制池独立能防止业务池耗尽直接占掉控制预算；网络写锁、CPU、QUIC 连接窗口仍然共享，不能保证任意压力下控制消息立即送达。

## 执行和排队

`Stats.InFlight` 包括已接纳排队、执行和终态 ACK/FIN 等待。`Executing` 仅统计已分配执行名额的处理器；处理器尚未返回时仍计费。终态等待不占执行名额，但继续占在途名额。`Queued` 是实际等待集合；队列本身不为每个请求创建等待 goroutine，以 Context callback 响应取消/超时。

调度受全局 MaxExecuting 和方法 Limit 双重约束。QueueCapacity 是所有方法共享的有界队列，0 表示使用默认 64，关闭排队请使用 Governance=nil。默认 QueueTimeout=100ms、SampleWindow=16、MaxExecuting=16。配置都受 MaxInFlight 及显式范围约束，策略列表会复制，方法注册在 Serve 冻结。

FIFO 总是选择最早的可执行请求；某方法限额满不会故意堵住后面可执行方法。MethodFair 在有积压时按服务端方法权重 1..16 轮转，每个方法内 FIFO。队列满而该方法仍可立即执行时，不会浪费空闲执行槽；队列满且无执行容量则拒绝新请求。它没有各方法独立排队配额，单方法仍可能占满公共队列；权重不是租户/CPU/token 成本公平，不承诺严格墙钟等待上界。

排队中的取消和队列/调用预算到期直接移出，不调用处理器。取得执行槽后、真正调用前再次核对取消及 deadline。drain 先封闭新接纳，再允许已接纳队列继续执行并等待 ACK/FIN；截止后取消和强制回收 IO，无法强制终止忽略 Context 的用户函数。取消/执行/终态统一使用原请求状态机，没有业务重放。

## 延迟控制和压力反馈

目标是方法**处理器驻留时间**的窗口 p90，不是端到端 SLO：包含处理器内部等待与流信用等待，排除队列、ACK、FIN。默认方法 Min=1、Initial=Max=全局执行槽、Target=20ms；可逐方法配置。每个有效窗口超目标或出现 overload/unavailable 时，Limit 乘 0.8 并向下取整，最低 Min；窗口正常且观察到需求时加 1，最高 Max。已有执行不会因限额降低而被抢占。

成功、执行过久的 deadline 及 overload/unavailable 参与采样；用户取消保持中性。决策环只保留最近 128 次变化并记录覆盖数量。控制器不估计真正 CPU 服务时间、token 成本或外部依赖瓶颈；目标低于固有耗时会持续降到 Min，静态合适限额可能更好。Target 与 Min/Max 应结合实际工作量和静态基线配置。

END 可携带 v/epoch/seq/ttl_ms/path 及方法限额、执行、队列、活跃需求物理连接数、建议值、接收字节使用。建议值为方法 Limit / 有需求连接数，向下取整且至少 1；空闲 TCP 连接和 QUIC 原生流数量不参与除数。需求不是长期租约，连接可能变化，多连接客户端可过度分摊，建议总和不保证不超过服务端限额。

发送 END 时检查实际 JSON 编码大小；错误诊断文本的转义可能比原始文本更大。超过帧/控制上限时按 UTF-8 边界缩短可选 message，必要时移除可选 pressure，保持状态码和回执。诊断不能依赖完整错误文本；反馈缺失时客户端沿用静态上限。

客户端按物理连接/方法校验 epoch、路径、版本、序列及数值范围；跨方法序列不互相覆盖。TTL 在 50ms..5s 内且不能超过客户端 MaxAge，先校验整数再转换 duration。为了不假设跨机时钟或把迟到元数据重新续命，客户端从 TTL 中保守扣除整个本地调用年龄，之后按本机单调时间继续过期。长调用可能因此丢弃其实刚生成的建议，这是明确的保守取舍。

有效建议限制方法并发，拒绝在业务 OPEN 写入之前返回本地 ResourceExhausted；QUIC 此时可能已分配一个原生流。不同子交换共用同一反馈状态，完成通知前释放建议名额。失效、缺失、乱序、异常或容量外元数据不扩大权限，回退本地静态在途上限；MaxMethods 默认 128，满且无法回收过期非活跃项时不新增元数据状态。新 Dial 新建 epoch 状态，没有自动重连。

压力反馈用于显式选择的受信任服务端。连接认证、方法授权和多连接配额的配置责任见 [使用约束](operating-notes.md)。

## 可复跑验证

```sh
python3 scripts/verify-discovery.py --output /tmp/kanarpc-c-test
python3 scripts/verify-discovery.py --output /tmp/kanarpc-c-race --race
python3 scripts/verify-discovery.py --output /tmp/kanarpc-c-min --toolchain go1.26.0 --race
go test ./internal/wire -run='^$' -fuzz='^FuzzReadReserved$' -fuzztime=15s -parallel=2
go test . -run='^$' -fuzz='^FuzzPressure$' -fuzztime=15s -parallel=2
python3 scripts/verify-governance.py --output /tmp/kanarpc-c-control --cross-transport
```

最后的对照是独立双进程开放到达负载；默认 4 场景 × 5 策略 × 3 种子，再补 TLS/QUIC 反馈链路。固定低并发策略是更强的已知容量参照。完整 offered、生成端迟到/拒绝、本地拒绝、远端超时、成功分位数、带时间戳的状态采样、CPU、决策与资源尾部保留，不用只统计成功延迟来掩盖丢弃。需要脚本自身 race 验证时可加 `--race`，该轮时间不可用于性能结论。

本阶段的测量使用本机 RPC 与定时器/动态槽位负载。结果、修复前故障和退化数据见 [R7C 报告](verification/governance-r7c/report.md)；环境与测量口径见 [使用约束](operating-notes.md)。
