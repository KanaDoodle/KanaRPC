# KanaRPC

上下文优先的实验 Go RPC 框架，支持普通/服务端流式调用、TCP/TLS、每请求一条原生流的 QUIC、etcd 注册发现，以及服务端反馈驱动的并发治理。通过明确的请求状态、资源归属和故障对照验证取消、流控与关闭行为。

新版在独立目录重新实现 v2 协议及内核，不导入旧 KanaRPC-Go 实现，与旧 wire protocol 不兼容。开发阶段、实现来源和第三方依赖记录在 [ORIGINS.md](ORIGINS.md)；仓库维护与独立构建方式见 [开发说明](docs/development.md)。

当前阶段重点是可解释的请求状态和资源归属。方法通过泛型辅助函数显式注册，所有处理器接收 `context.Context`；普通与流式调用使用同一协议生命周期；执行、排队与终态等待分别统计，记录缓冲有共享字节预算；成功终态带随机回执并等待客户端协议 ACK。

```go
server, _ := kanarpc.NewServer(kanarpc.ServerConfig{})
err := kanarpc.RegisterUnary(server, "/math/add", func(ctx context.Context, in [2]int) (int, error) {
    return in[0] + in[1], ctx.Err()
})
// Check err, create a net.Listener, then server.Serve(listener).
// client, err := kanarpc.Dial(ctx, address, kanarpc.ClientConfig{})
// err = client.Call(ctx, "/math/add", [2]int{1, 2}, &sum)
```

Go 1.26.0 或更新版本。本地验证：

```sh
go test ./...
go test -race ./...
python3 scripts/verify-core.py --output /tmp/kanarpc-core-new-run
python3 scripts/verify-discovery.py --output /tmp/kanarpc-real-etcd --race
python3 scripts/verify-transports.py --output /tmp/kanarpc-transports
python3 scripts/verify-governance.py --output /tmp/kanarpc-governance --cross-transport
```

`verify-core.py` 启动两个独立进程，在真实 loopback TCP 上验证普通调用、20 个流式 chunk、用户取消，并在已接纳的流执行中发 SIGTERM。`verify-discovery.py` 要求本机 etcd，创建独立单成员实例并实际执行 lease/watch/compaction；直接 `go test` 未配置该服务时会跳过相应用例。`verify-transports.py` 比较 TCP/TLS/QUIC，`verify-governance.py` 比较固定并发/FIFO、公平、自适应和反馈辅助，保留全部 offered 与退化结果。数据与定时器是 fixture，没有调用模型。输出目录必须不存在，二进制和日志归该验收目录所有。

当前没有自动重试、隐式降级或业务重放。调用失败不能据此推断服务端没有执行；ACK 证明协议终态标识被收到，不证明业务提交或应用消费。

原 `KanaRPC-Go` 保留为衍生学习项目及历史对照，不删除其来源说明。这里不是把旧目录改名；实现来源、依赖及声明边界见 [ORIGINS.md](ORIGINS.md)。R7B 明确使用 quic-go 与 etcd client，不导入旧 RPC 实现；嵌套的 `tools/lifecycle-compare` 仅为对照实验引用旧项目。

R7B 的 API、关闭协议与发现新鲜度见 [传输与发现](docs/transport-r7b.md)，R7C 配置与控制边界见 [过载治理](docs/governance-r7c.md)，R8A 已增加 [控制回退与复位](docs/control-r8a.md)并完成[验收](docs/verification/control-r8a/report.md)。后续优先 RPC 自身的 R8B 编码前资源接纳、R8C 发现驱动连接生命周期、R8D 真实网络验收。R7D CampusTrace 迁移按用户要求暂缓；旧版本的功能清单、294 项检查和 R6 性能结果不能计为新版验收。尚未发布稳定版本，CampusTrace 默认依赖未修改。不是生产框架。

设计与后续步骤：[重写计划](docs/rewrite-plan.md)、[v2 协议](docs/protocol-v2.md)。实际验收与遗留风险：[R7A 报告](docs/verification/core-r7/report.md)、[R7B 报告](docs/verification/transport-r7b/report.md)、[R7C 报告](docs/verification/governance-r7c/report.md)、[R8A 报告](docs/verification/control-r8a/report.md)。

许可沿用 AGPL-3.0，标准许可证正文在 [LICENSE](LICENSE)。依赖库和通用算法不属于本项目原创发明。
