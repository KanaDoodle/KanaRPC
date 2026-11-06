# KanaRPC

**KanaRPC 是一个 Go RPC 框架，在过载时限制请求接纳，为并发调用分配有界资源，并跟踪取消与停机时的交付结果。** 支持普通调用、服务端流式调用、JSON / Protobuf 和 TCP/TLS / 原生 QUIC。

[本地演示](#快速体验) · [业务接入](#业务接入) · [压测与验证](#压测与验证) · [架构与协议](#架构与调用链)

## 核心设计

| 问题 | 实现 | 证据 |
|---|---|---|
| 慢方法积压时怎样限制新请求 | 服务端按方法加权调度，用有界队列限制等待，按处理器耗时通过 AIMD 调整执行并发。客户端用短效反馈在编码前接纳或拒绝 | [过载对照实验](docs/verification/governance-r7c/report.md) |
| 请求数有限，怎样约束并发编码和未读数据 | 连接和连接池有字节预算，流式发送受信用窗口限制。未读流、进行中的解码和退休连接持有的缓冲继续计入预算，直至释放 | [池级预算与故障测试](docs/verification/pool-r9a/report.md) |
| 停机时怎样知道哪些请求已经收尾 | 统一排队、远端取消、终态回执和资源释放。`DrainWithReport` 分别报告资源是否退出和协议交付故障，业务提交由应用确认 | [关闭与故障注入](docs/drain-r9c.md) |

## 快速体验

需要 Go 1.26.10+，推荐 Go 1.27.3 或所在分支的最新安全补丁。在仓库根目录运行：

```sh
go run ./cmd/showcase
```

打开 [127.0.0.1:8080](http://127.0.0.1:8080/)，通过三组操作观察一次请求如何经过框架。

1. **制造过载**。调整请求速率、慢请求比例和后端槽位，观察并发上限、排队与客户端提前拒绝。
2. **取消流式请求**。收到片段后取消，沿时间线核对取消信号、处理器返回和资源释放。
3. **执行优雅关闭**。注入一次终态未回执故障，分别查看资源退出和交付结果。

控制台使用本机 QUIC 和可调的模拟负载，启动无需 etcd、容器或 npm。[操作指南](docs/showcase-r10.md) · [界面截图（注入未回执故障）](docs/verification/showcase-r10/showcase-final.png) · [链路验证](docs/verification/showcase-r10/report.md)

## 业务接入

CampusTrace 的 Worker 通过 KanaRPC 调用岗位分析服务，使用 TCP 普通调用、etcd 服务发现、连接复用、固定执行上限和远端取消。业务重试与写入回执由 CampusTrace 管理。[接入设计与验收](https://github.com/KanaDoodle/CampusTrace/blob/main/docs/kanarpc-migration.md)

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

运行 `go run ./cmd/protobuf-demo` 可体验生成 SDK 的普通调用、流式接收与远端取消。`.proto` 定义、代码生成和连接池接入见 [Protobuf 使用指南](docs/protobuf.md)，让类型帮你记住方法和参数 (•̀ᴗ•́)و

准备接进独立应用时，按 [SDK 治理与发布](docs/sdk-r12.md) 固定版本、检查生成代码和 schema 兼容性。历史验收证据保留在仓库中，Go 模块下载包专注 SDK 源码与使用文档。

## 跑通业务接入链

```sh
go run ./cmd/production-demo
# 也可以选择 -transport tls 或 -transport quic。
```

示例在本机完成鉴权、类型化请求、流式响应、压缩、追踪和关闭。接入选项、临时证书和调试 CLI 见[完整调用链](docs/production-r15.md)。

## 架构与调用链

根包负责请求状态机、编解码、传输适配和资源治理；`discovery` 提供 etcd 注册与快照；`routing` 消费发现快照并管理连接。QUIC 传输基于 quic-go，注册发现基于 etcd client。

一次请求会走过这条路线：**业务管线与凭证 → 路由 / 限流 / 总 deadline → 客户端接纳与编码预留 → 编码与按需压缩 → 服务端接纳与方法调度 → 鉴权 / 解压 / 执行 / 信用流控 → END 与协议回执 → 请求退休与资源释放**。取消、截止时间和连接故障通过同一状态机参与终态结算。编码准备、排队、执行、回执等待与未读数据分别统计，便于定位压力来源。

| 文档 | 内容 |
| --- | --- |
| [架构与演进](docs/rewrite-plan.md)、[KNR2 协议](docs/protocol-v2.md) | 模块职责、请求状态与帧契约 |
| [业务接入与完整调用链](docs/production-r15.md) | 元数据、中间件、鉴权、gzip、观测、就绪发布和调用策略 |
| [Protobuf 与 Go SDK](docs/protobuf.md)、[组件评估](docs/components-20261101.md) | 接口定义、二进制负载、生成代码与依赖升级 |
| [传输与发现](docs/transport-r7b.md)、[连接管理](docs/connections-r8c.md) | TLS/QUIC、etcd 一致性与连接归属 |
| [过载治理](docs/governance-r7c.md)、[控制回退](docs/control-r8a.md) | 调度、延迟目标、压力反馈与复位 |
| [编码前接纳](docs/resources-r8b.md)、[共享预算](docs/pool-r9a.md) | 字节租约、未读流与退休对象统计 |
| [关闭与故障计数](docs/drain-r9c.md)、[使用约束](docs/operating-notes.md) | 取消、回执、关闭结果与部署要求 |
| [网络诊断](docs/network-r8d.md)、[QUIC 发送](docs/quic-r9b.md)、[小请求分析](docs/quic-r9d.md) | 根因、内部基线和优化取舍 |

## 压测与验证

| 验证场景 | 结果 |
|---|---|
| 过载与恢复 | 在本机容量切换实验中，相比仅服务端自适应，加入客户端反馈后按时完成数增加 13.4%，成功总数下降 3.6%。提前拒绝部分请求，减少等待。[实验与退化样本](docs/verification/governance-r7c/report.md) |
| TCP 流式优化 | R14 本机双进程、并发 16、每次 8 × 1 KiB 的流式调用，内核优化后吞吐由 18,226 升至 24,695 RPC/s，P95 由 1.21 ms 降至 0.85 ms，均为四轮中位数。相同条件下 grpc-go 吞吐为 74,306 RPC/s。[同条件对照](docs/grpc-optimization-r14.md) |
| 后续调用管线回归 | R15 默认路径复测中，TCP 短流吞吐相对 R14 的配对变化中位数为 −9.76%。[完整结果](docs/verification/production-r15/README.md) |

```sh
GOWORK=off go build -mod=readonly ./...
GOWORK=off go test ./...
GOWORK=off go test -race ./...
```

etcd 集成用例需要独立实例，可通过 [开发说明](docs/development.md) 中的 runner 自动创建和回收。各阶段的环境、源码快照、完整请求结果、失败与退化数据保存在 [验证索引](docs/verification/README.md)。已有测量来自本机 loopback 与本地 Linux VM；真实双机、WAN、多成员 etcd 和模型推理性能尚未验证。

普通与流式调用的 TCP/TLS 外部基线见 [grpc-go 同条件对照](docs/grpc-comparison-r13.md)，包含完整矩阵、CPU/分配开销及不利结果。后续的 profile 定位、信用合并与 TCP writev 记录见 [并发与流式优化](docs/grpc-optimization-r14.md)。

## 兼容性与许可

KNR2 v2 使用独立的协议和运行内核，与旧 KanaRPC-Go wire protocol 分离。接入时显式选择端点与传输；业务幂等、方法鉴权及重试策略由应用管理。其他资源、取消和回执契约统一见 [使用约束](docs/operating-notes.md)。

项目采用 [AGPL-3.0](LICENSE)。实现沿革、旧项目关系和第三方依赖记录见 [ORIGINS.md](ORIGINS.md)。
