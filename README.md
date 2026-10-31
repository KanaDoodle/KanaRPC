# KanaRPC

> 让请求好好出发，也好好收尾。 (｡•̀ᴗ-)✧

以请求生命周期与资源治理为核心的 Go RPC 框架，提供普通调用、服务端流式调用、JSON / Protobuf 编码、TCP/TLS 与原生 QUIC 传输。取消、排队、执行、终态回执和资源释放使用统一状态机，方便追踪每次调用的进展与资源归属。

## 核心能力 🧩

| 能力 | 设计与用途 |
| --- | --- |
| 上下文驱动的调用 | 泛型显式注册处理器，传播请求预算与取消信号；流式响应通过信用窗口控制发送 |
| Protobuf 类型化 SDK | 从 `.proto` 生成 Go 消息、客户端与服务注册，兼容 Editions/Opaque；建连时协商能力和帧上限，schema 变更按二进制、JSON 与源码分别检查 |
| 原生 QUIC 请求流 | 每次 RPC 使用独立双向流，隔离请求流上的坏帧与取消；TLS/QUIC 显式配置证书信任，支持 mTLS |
| 自适应过载控制 | 方法加权调度、延迟窗口 AIMD 和短效压力反馈协同控制服务端执行与客户端接纳；支持固定限额回退与人工复位 |
| 池级资源预算 | 编码前接纳，编码、接收、发送采用连接与池两层字节租约；退休连接、未读流和进行中的解码持续计费 |
| 发现驱动的连接管理 | etcd lease/watch、快照新鲜度校验、建连去重、有界连接数和 endpoint 退休，保护在途调用与未读结果 |
| 可报告的优雅关闭 | `DrainWithReport` 分别报告资源退出、历史故障和本次新增故障，保留未回执与交换未确认的累计记录 |
| 请求决策时间线 | 可选的有界本地事件记录器，关联接纳、调度、执行、取消、回执和释放，配套交互式治理控制台 |

## 快速体验 🐾

需要 Go 1.26.10+，推荐 Go 1.27.3 或所在分支的最新安全补丁。在仓库根目录运行：

```sh
go run ./cmd/showcase
```

打开 `http://127.0.0.1:8080`，给请求安排一趟小旅行：

1. **加点压力**：调节请求速率、慢请求比例、处理耗时和后端槽位，观察方法并发与客户端反馈。
2. **随时喊停**：发起流式请求，接收片段后点击取消，沿时间线查看取消控制、处理器返回和资源释放。
3. **认真收尾**：执行优雅关闭，或注入未回执故障，分别查看资源状态与交付结果。

控制台使用本机 QUIC 连接和定时器/槽位负载，启动无需 GPU、etcd、容器或 npm。准备好，动动旋钮吧 (ง •̀_•́)ง

[控制台操作指南](docs/showcase-r10.md) · [页面与链路验证记录](docs/verification/showcase-r10/report.md)

## 接进你的项目

模块路径为 `github.com/KanaDoodle/KanaRPC`。处理器显式接收 `context.Context`：

```go
server, err := kanarpc.NewServer(kanarpc.ServerConfig{})
// 检查 err 后注册方法。
err = kanarpc.RegisterUnary(server, "/math/add", func(ctx context.Context, in [2]int) (int, error) {
    return in[0] + in[1], ctx.Err()
})
// 检查 err，创建 net.Listener，再调用 server.Serve(listener)。
// 客户端：
// client, err := kanarpc.Dial(ctx, address, kanarpc.ClientConfig{})
// err = client.Call(ctx, "/math/add", [2]int{1, 2}, &sum)
```

这是注册与调用片段；完整可运行示例见 [连接管理](routing/example_test.go) 和 [关闭报告](drain_example_test.go)。服务端流使用 `RegisterStream`，客户端通过 `Stream/Recv` 消费结果，提前结束时调用 `Close`。版本选择与独立消费模块的接入方式见 [开发说明](docs/development.md)。

偏爱先写接口契约？运行 `go run ./cmd/protobuf-demo`，体验生成 SDK 的普通调用、流式接收与远端取消。`.proto` 定义、代码生成和连接池接入见 [Protobuf 使用指南](docs/protobuf.md)，让类型帮你记住方法和参数 (•̀ᴗ•́)و

准备接进独立应用时，按 [SDK 治理与发布](docs/sdk-r12.md) 固定版本、检查生成代码和 schema 兼容性。历史验收证据保留在仓库中，Go 模块下载包专注 SDK 源码与使用文档。

## 架构：请求的内部旅程

根包负责请求状态机、编解码、传输适配和资源治理；`discovery` 提供 etcd 注册与快照；`routing` 消费发现快照并管理连接。QUIC 传输基于 quic-go，注册发现基于 etcd client。

一次请求会走过这条路线：**客户端接纳与编码预留 → 发送 OPEN → 服务端接纳与方法调度 → 执行 / 信用流控 → END 与协议回执 → 请求退休与资源释放**。取消、截止时间和连接故障通过同一状态机参与终态结算。编码准备、排队、执行、回执等待与未读数据分别统计，便于定位压力来源。

| 文档 | 内容 |
| --- | --- |
| [架构与演进](docs/rewrite-plan.md)、[KNR2 协议](docs/protocol-v2.md) | 模块职责、请求状态与帧契约 |
| [Protobuf 与 Go SDK](docs/protobuf.md)、[组件评估](docs/components-20261101.md) | 接口定义、二进制负载、生成代码与依赖升级 |
| [传输与发现](docs/transport-r7b.md)、[连接管理](docs/connections-r8c.md) | TLS/QUIC、etcd 一致性与连接归属 |
| [过载治理](docs/governance-r7c.md)、[控制回退](docs/control-r8a.md) | 调度、延迟目标、压力反馈与复位 |
| [编码前接纳](docs/resources-r8b.md)、[共享预算](docs/pool-r9a.md) | 字节租约、未读流与退休对象统计 |
| [关闭与故障计数](docs/drain-r9c.md)、[使用约束](docs/operating-notes.md) | 取消、回执、关闭结果与部署要求 |
| [网络诊断](docs/network-r8d.md)、[QUIC 发送](docs/quic-r9b.md)、[小请求分析](docs/quic-r9d.md) | 根因、内部基线和优化取舍 |

## 开发与验证

```sh
GOWORK=off go build -mod=readonly ./...
GOWORK=off go test ./...
GOWORK=off go test -race ./...
```

etcd 集成用例需要独立实例，可通过 [开发说明](docs/development.md) 中的 runner 自动创建和回收。各阶段的环境、源码快照、完整请求结果、失败与退化数据保存在 [验证索引](docs/verification/README.md)。已有测量来自本机 loopback 与本地 Linux VM；真实双机、WAN、多成员 etcd 和模型推理性能尚未验证。

## 兼容性与许可

KNR2 v2 使用独立的协议和运行内核，与旧 KanaRPC-Go wire protocol 分离。接入时显式选择端点与传输；业务幂等、方法鉴权及重试策略由应用管理。其他资源、取消和回执契约统一见 [使用约束](docs/operating-notes.md)。

项目采用 [AGPL-3.0](LICENSE)。实现沿革、旧项目关系和第三方依赖记录见 [ORIGINS.md](ORIGINS.md)。
