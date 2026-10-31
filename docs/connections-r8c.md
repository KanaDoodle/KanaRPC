# R8C：发现驱动的连接生命周期

routing 将发现快照、建连去重和资源退休组合为有界的连接生命周期，减少应用逐次选址与 Dial 的管理工作。

| 阶段 | 实施 | 复现/对照 |
| --- | --- | --- |
| C1 | 独立 routing 包消费 discovery.Snapshot，显式 transport、完整快照/版本校验、每次交给根客户端前刷新 | 未接入管理时需要手动选址/Dial；陈旧/不一致快照不能继续路由；不是逐 wire 发送的分布式线性一致保证 |
| C2 | 有界连接与管理调用数，按 endpoint 去重建连、候选轮转、等待取消、失败 backoff | 32 个并发首次调用只建一条连接；一次等待者取消不杀其他等待者；全取消/迟到建连/关闭/节点 ABA |
| C3 | 删除或地址变化后旧连接退休，保护已接纳调用与已结束未消费流；显式 Close/Wait | 直接 Close 旧客户端会丢弃已完成未读缓冲的根 API 基线；新流包装直到 EOF/error/Close 才释放引用；容量不足时有界等待 |
| C4 | 真实 etcd + TCP/TLS/QUIC、故障/端点更新、重复执行边界，普通/race/fuzz与独立进程基线 | 更新/删除不导致业务重放；比较逐次 Dial 和连接复用，保留发现检查、流引用和连接 churn 的代价 |

设计选择：routing 依赖根 RPC 和 discovery，避免根包与 discovery 的导入循环。Resolver 仍由创建者拥有，连接管理不擅自关闭它。发现失效时停止新路由，已在途工作继续，空闲连接可暂时停用并等待新鲜快照；明确删除才退休。连接/建连/关闭中对象统一占硬上限，不靠无限后台重连。

`routing.Stream` 包装根 Stream 并保护未读结果，具有独立返回类型。Close 取消管理调用与 IO，Wait(ctx) 等待未退还引用和用户编码结束。

## 正式接入与所有权

从独立模块 `github.com/KanaDoodle/KanaRPC` 导入 `discovery` 与 `routing`。使用经审查提交的 pseudo-version 固定版本；本地开发可在独立消费模块中用 `replace` 指向 checkout。版本和接入说明见 [开发指南](development.md)。

```go
registry, err := discovery.New(ctx, discovery.Config{
    Endpoints: []string{"http://127.0.0.1:2379"},
    Namespace: "/my-app/rpc-v2",
})
// 每一步都应检查 err。
resolver, err := registry.Watch(ctx, "echo-service", rpc.TCP)
client, err := routing.New(ctx, resolver, routing.Config{
    Transport: rpc.TCP,
    MaxConnections: 8, MaxInFlight: 128, MaxEndpoints: 128,
    Dial: rpc.DialOptions{Client: rpc.ClientConfig{Timeout: 3 * time.Second}},
})
err = client.Call(ctx, "/demo/echo", request, &response)
stream, err := client.Stream(ctx, "/demo/items", request)
// 对 stream 使用 Recv，直到 EOF/error；提前结束必须 Close。
// 创建 stream 的 Context 控制取消，不能在 Stream 返回后立刻 cancel。
```

以上是接入片段；可编译并实际运行的公共 API 示例见 [routing/example_test.go](../routing/example_test.go)。服务端仍需显式 RegisterUnary/RegisterStream 并使用 Registry.Register 发布 endpoint。TLS/QUIC 必须显式配置可信 CA、ServerName 和需要的客户端证书，管理层不生成证书，也不降级 TCP。

退出顺序是：停止业务接纳；关闭应用持有的 stream；`client.Close()`，用独立且未取消的 deadline Context 调用 `client.Wait(ctx)`；最后由创建者关闭 resolver/registry。不要把已经取消的请求 Context 当作 shutdown 等待预算。Client 不持有 resolver 的关闭权限。

## 资源与失败契约

- `MaxConnections` 默认 8，范围 1..256；建连、退休和关闭中的对象统一计入。`MaxInFlight` 默认 128，范围 1..4096，包含等待、普通调用、尚未 EOF/Close 的流和进行中的解码。`MaxEndpoints` 默认 128，范围 1..4096，限制单次有效快照。源返回快照所需的内部内存不属于连接池预算。
- `PollInterval` 默认 50ms；每次调用在选择、等待和交给根客户端前还会刷新。Ready=false 时停用新路由并停放现有连接；显式删除/地址变更才退休。快照要求合法 ID/address/transport、无重复 ID、正 revision/cluster、cluster 不变、revision 不倒退，同版本内容相同。更新仍可能在最后一次检查之后发生，这是接纳时快照门槛，不是发送时的分布式线性一致性保证。
- `DialTimeout` 默认 1s；共用建连使用池生命周期预算，单个等待者取消不杀其他等待者，全部离开才取消该建连。`DialBackoff` 默认 100ms，是有界固定退避；没有抖动、指数增长、后台重连或同一业务调用的自动 fallback。下次调用可在退避后重新建连。
- 候选按 endpoint ID 排序并轮转，不按推断出的后端负载打分。容量满时仅淘汰无引用的最久未用连接；等待也占管理调用名额并受总调用预算限制。候选轮转不承诺连接等待者之间的 FIFO 公平。连接数少于活跃节点数会造成握手 churn；应按实际服务规模配置并观察 dials。
- `AdmissionError` 仅用于管理层在交给根 Caller 之前的策略拒绝，可用 `errors.Is` 区分 ErrNotReady/ErrSnapshot/ErrCapacity/ErrBackoff/ErrRetired，并可展开为 RPC Status。原始建连错误原样返回。交给根客户端后的错误保持原状，不能据此推导业务未执行；需要幂等性时由业务提供幂等键和服务端去重。
- `Stats` 包含已管理连接、dialing/retiring/closing、调用名额、峰值、dials/failures/coalesced/reused、revision/cluster。Rejected 只统计管理名额不足。发现状态指标是最近一次检查；实际新调用始终检查源。已知根客户端关闭时只淘汰连接，不重放触发错误的请求。
- `routing.Stream` 在 EOF/error/Close 后退还引用，进行中的 Recv（含同步用户 UnmarshalJSON）仍占用引用。结束但未读取的流不能仅凭根 Pending=0 退休。父管理器 Close 会关闭 IO/丢弃未读缓冲，应用仍须归还 stream；Wait 的 deadline 能看到遗忘引用、阻塞编码/解码或违约 Snapshot 的残留。

Source.Snapshot 必须快速、非阻塞，并返回调用期间稳定的快照。实际 discovery.Resolver 复制且排序快照，自己实施 TTL/线性化 resnapshot/watch。自定义 Source 内部阻塞无法被 Go 强杀；管理器不会每轮派生不可回收的读源 goroutine。配置应在 New 时提供，TLS 配置中的证书材料和回调遵循 Go TLS 的使用后不可并发修改契约。

## 验证入口

```sh
python3 scripts/verify-discovery.py --output /tmp/kanarpc-r8c-checks --race
go test ./routing -run='^$' -fuzz='^FuzzSnapshotFence$' -fuzztime=15s -parallel=2
python3 scripts/verify-routing.py --output /tmp/kanarpc-r8c-controls
python3 scripts/verify-sdk.py --output /tmp/kanarpc-r8c-sdk
```

`verify-routing.py` 用两个独立真实 worker 和一个独立 client 比较逐请求 Dial、直接复用两条连接、管理两条连接、两节点只配一条连接。性能输入使用固定本地 Snapshot，真实 etcd 的增删/流式/所有权另由集成测试验证。所有 offered、成功数、错误、握手数和进程 CPU 保留；race 计时不作性能证据。

`verify-routing.py` 完成整个计划矩阵并保留失败 case：完整输出但部分 offered 失败且无损坏/重复/资源残留记为 DEGRADED；预热失败、崩溃、损坏或未收敛记 FAIL，继续其他 case 后以非零状态退出。预热失败时计划测量请求尚未启动，不能把计划数充当实际 offered。SDK 入口另建消费模块，用本地 replace 与 GOWORK=off 验证公共 API。最终结果和已观察到的 QUIC 建连失败见 [报告](verification/connections-r8c/report.md)。
