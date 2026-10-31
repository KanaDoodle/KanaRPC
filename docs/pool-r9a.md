# R9A：池级共享字节预算与退休对象统计

从 R8D `90af591` 开始，仅修改新版 KanaRPC。CampusTrace 不动；真实双机未提供，本轮不将 loopback/fixture 当物理网络或推理收益。

| 阶段 | 结合当前代码的实现步骤 | 复现和验收 |
| --- | --- | --- |
| A1 | 冻结基线；复现两个 endpoint 各自持有编码/未读缓冲，确认按连接的额度相加会越过拟设池上限；确认现有 Retiring 只有对象数，不能解释保留字节 | 阻塞编码器、已完成未读流、地址更新、阻塞用户解码；先保存失败/观测，不修改问题无关代码 |
| A2 | 根包新增显式共享 MemoryPool/MemoryAccount，连接局部额度与池额度在同一锁内接纳/缩减/释放；编码/接收/发送共用数据总额，控制收发有独立保留额度 | 容量竞争、取消、panic、部分帧、失败建连、零字节/重复释放、跨连接释放唤醒；不先占一个层级再无限等待另一个层级 |
| A3 | routing 默认创建池，每个 dialing/active/retiring/closing 对象持有独立 account；退休转移统计而不释放实际租约；未读量作为 Receive 的子集单列 | 旧 endpoint 退休仍保留未读流与解码中的字节；Close/Wait 和最终回收；迟到 Dial、ABA、连接数/账户数上限；有界对象明细，不累积无限历史 |
| A4 | TCP/TLS/QUIC、etcd 更新、race/fuzz、预算饱和和周期压力；独立进程对照旧版、直接复用与新版共享池 | 保留拒绝、超时、吞吐/延迟退化；冻结最终源码后串行性能对照。有限资源观察不证明 RSS 上限或长期生产无泄漏 |

设计约束：保留根客户端各自的 bufferOwner，关闭一个连接不能清空其他连接的结果。共享的是原子字节租约和统计，不是请求表、取消上下文或未读队列。任意用户 MarshalJSON/UnmarshalJSON 的内部临时分配、TLS/QUIC 内部内存和 RSS 不受该预算硬限制。

接收/发送延续现有 fail-fast 行为；编码容量在已经受管理调用数限制的调用者中可取消等待。数据预算耗尽不能直接耗用控制保留额度；控制额度本身也有限，不承诺无限故障下控制帧必达。请求仍只交给根客户端一次，不增加业务重放。

## 已实现的配置与归属

`routing.Config.Memory` 是池总额，`Dial.Client.Memory` 继续限制每个根客户端。两层必须同时满足；不再把每条连接的额度直接相加作为池额度。默认池数据总额 32 MiB，编码/接收/发送各自上限 16 MiB，控制收发各保留 64 KiB；`Total.Limit = Data.Limit + 2*ControlBytes`。这会改变多连接满载下的接纳结果，应按工作量显式配置。提高 MaxFrameBytes 时需要同步提高池编码与总额；构造时不够会直接返回配置错误。

```go
client, err := routing.New(ctx, resolver, routing.Config{
    Transport: kanarpc.QUIC,
    Dial: kanarpc.DialOptions{TLS: kanarpc.TLSOptions{Config: tlsConfig}},
    MaxConnections: 8,
    MaxInFlight: 128,
    Memory: kanarpc.MemoryPoolConfig{
        DataBytes: 32 << 20,
        Memory: kanarpc.MemoryConfig{
            EncodingBytes: 16 << 20,
            ReceiveBytes: 16 << 20,
            SendBytes: 16 << 20,
            ControlBytes: 64 << 10,
        },
    },
})
// Check err. Stream must be consumed or closed, including after remote EOF.
// client.MemoryStats(): one atomic pool snapshot, including retired accounts.
// client.ConnectionStats(): at most MaxConnections ownership rows, sorted by ID.
// client.Close(); client.Wait(freshDeadlineContext)
```

编码先预留 `2*MaxFrameBytes`，编码完成后缩减；发送再取得帧加 header 的租约。池编码额度至少容纳两帧，数据总额至少 `3*MaxFrameBytes+24`；这是编码与总额的必要头寸检查，不保证具体请求一定获准；接收/发送分类及连接额度可以更小，满载也仍会拒绝。接收和发送 fail-fast，不新增暗中排队或业务重放。编码在现有受限调用者内等待共享 changed 通知，取消/关闭解除等待；没有每个等待者的后台 goroutine。

调用者也可以直接创建 `NewMemoryPool(config, maxAccounts)`，每次 Dial/NewClient 使用一个 `NewAccount()` 作为 `ClientConfig.MemoryAccount`。一个账户只绑定一个公开根客户端，QUIC 的请求子流共用父账户。失败建连后释放未绑定账户；成功后先 Close，等待用户编码/解码和租约真正结束，再 Release。`Retire` 只改变归属，`Seal` 只禁止新账户，不抢走现有控制/数据租约。routing 自动执行这些步骤，禁止外部账户注入，通常应直接用 routing。

## 统计定义

- `Data` 为 Encoding + Receive + Send，`Total` 再加控制收发。原子快照满足 `Active.DataBytes + Retired.DataBytes == Data.Used`。
- `Unread` 是已经入队而尚未取出的 DATA payload，为 Receive 的子集。取出后用户解码还没返回，Unread 会减少而 Receive 仍计费。不要把二者相加。
- 活跃/退休分组提供账户、租约、编码等待者、各类当前字节、未读流/块/字节；账户和全池还保留各自峰值。分组不通过相加各账户历史峰值伪造同时峰值。
- `ConnectionStats` 包含 endpoint、唯一账户 ID、dialing/retiring/closing/closed、引用数、退休持续时间和账户快照。地址 ABA 仍是两个不同对象。明细只保留尚被拥有的对象；Created/Released 是累计计数。
- `OwnerClosed` 表示公开父 Close 已完成 IO 清理；它不表示所有用户 codec 已返回。`Release` 还检查活跃租约、等待者和未读块，包括零字节控制租约。
- 各类 `Rejected` 与 `Total.Rejected` 是经过该池的拒绝尝试数，可能由连接局部额度、池分类/总额或已关闭账户触发；不能只据此推断全池该类已满。`Data.Rejected` 记录拒绝时数据总额不足的尝试。临时编码等待不计为拒绝。

统一锁保护池与附属局部租约；未附属客户端继续使用各自锁。锁顺序为 routing → pool、pending → bufferOwner → pool；pool 不回调 routing、pending 或 bufferOwner。一个连接关闭只清理自己的 bufferOwner，不能清空其他连接的结果。

## 重跑入口与边界

```sh
python3 scripts/verify-discovery.py --output /tmp/r9a-full --race
python3 scripts/verify-sdk.py --output /tmp/r9a-sdk
GOMAXPROCS=4 go test . -run '^$' -fuzz '^FuzzSharedByteLeases$' -fuzztime=30s -parallel=4
python3 scripts/verify-pool.py --output /tmp/r9a-pool --duration 180 --cycles 12 --tight-duration 30
```

`verify-pool.py` 使用各版本配对的 worker/client，同一份兼容驱动、两个真实 loopback worker 进程，发现输入为 snapshot fixture。输出包含源码 capsule/hash、逐轮强制 GC 观察、业务结果、退休持有与最终回收。PASS、DEGRADED（已回收但历史终态未回执导致 drain 返回错误）、FAIL 分开保存。它不是等 offered 的性能试验；有限时长、closed-loop offered 数、分配量或堆变化不能解释为性能收益或长期无泄漏。

预算覆盖框架记录缓冲及编码预留，不覆盖任意用户 MarshalJSON/UnmarshalJSON 内部分配、响应对象、所有管理对象开销、TLS/quic-go 内部内存或 RSS。用户 codec 可以不返回，忘记 Close 的流可以长期占住退休额度；本轮保证它们继续可见并受约束，不尝试强杀用户 goroutine。池锁和快照也有成本；共享编码等待不保证 FIFO/租户公平。真实双机、物理 NIC/WAN、多成员 etcd、GPU/vLLM 均未验证。CampusTrace 未修改。


压力复验额外定位到 quic-go v0.64.1 在并发 reset 与尾部小帧缓冲间的退休问题，可在框架计数归零时耗尽 native stream 额度。nativeChannel 通过非阻塞 identity WriteWithLimit 绕开该分支；没有更改依赖或重试业务。详细复现、隔离诊断、性能成本与原始失败见 [报告](verification/pool-r9a/report.md)。
