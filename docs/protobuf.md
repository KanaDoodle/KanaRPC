# Protobuf 与类型化 Go SDK

KanaRPC 可以从 `.proto` 生成消息、客户端和服务注册代码。业务代码通过类型化方法调用普通 RPC 和服务端流；TCP、TLS、QUIC 与发现连接池共用这套接口。JSON 注册与调用继续使用原有格式，二进制 Protobuf 按方法显式选择。

## 实施步骤与代码位置

| 步骤 | 实现 | 验证入口 |
| --- | --- | --- |
| 二进制负载与编码选择 | `codec.go`、`preparation.go`、`internal/wire` | `codec_test.go`：混合调用、空消息、错误编码、取消与预算 |
| 类型化客户端与原子注册 | `cmd/protoc-gen-kanarpc`、`internal/protogen` | 生成普通与服务端流接口；不支持的流方向在生成阶段报错 |
| 连接池与流归属 | `ServiceCaller/OpenStream`、`routing.Client` | `protobuf_sdk_test.go`：三种传输、节点移除、零字节未读响应与回收 |
| 独立应用接入 | `examples/echo`、`scripts/verify-protobuf.py` | 真实 protoc、可重复生成、独立消费模块、Editions/Opaque、取消与 drain |
| 组件升级复核 | Protobuf runtime、etcd client 与 Go 补丁版本 | [组件评估](components-20261101.md)与[验证记录](verification/protobuf-r11/report.md) |

## 运行示例

仓库包含生成文件，运行时无需安装 protoc：

```sh
go run ./cmd/protobuf-demo
```

程序启动自有 loopback TCP 服务，依次完成类型化普通调用、同连接 JSON 调用、三段流式响应、远端取消和带报告的关闭。示例服务定义在 [echo.proto](../examples/echo/echo.proto)，程序入口在 [main.go](../cmd/protobuf-demo/main.go)。

## 定义与生成

定义消息和方法，给字段分配稳定编号；删除字段后使用 `reserved` 保留编号与名称。需要区分“未提供”和零值时，proto3 使用 `optional`，Editions 使用显式 presence。二进制解码保留未知字段，消息演进仍应遵守 [Protobuf 官方兼容规则](https://protobuf.dev/programming-guides/dos-donts/)。

```protobuf
syntax = "proto3";
package app.echo;
option go_package = "example.com/myapp/pb";
message Request { string text = 1; optional string label = 2; }
message Response { string text = 1; }
service Echo {
  rpc Call(Request) returns (Response);
  rpc Watch(Request) returns (stream Response);
}
```

在应用自己的模块中固定 KanaRPC 版本后，构建与该版本配套的 RPC 插件。以下命令以 `example.com/myapp` 为应用模块路径，`service.proto` 位于模块根目录：

```sh
mkdir -p .tools/bin
GOBIN="$(pwd)/.tools/bin" go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.12
go build -o .tools/bin/protoc-gen-kanarpc github.com/KanaDoodle/KanaRPC/cmd/protoc-gen-kanarpc
protoc \
  --plugin="protoc-gen-go=$(pwd)/.tools/bin/protoc-gen-go" \
  --plugin="protoc-gen-kanarpc=$(pwd)/.tools/bin/protoc-gen-kanarpc" \
  --go_out=. --go_opt=module=example.com/myapp \
  --kanarpc_out=. --kanarpc_opt=module=example.com/myapp \
  service.proto
```

生成 `pb/service.pb.go` 与 `pb/service_kanarpc.pb.go`。插件使用官方 `protogen` 描述符 API，支持 proto3、Editions 2023/2024；普通调用和服务端流均生成类型化入口。生成器通过 `proto.Message` 操作消息，可配合 Open Struct 或 Opaque API。当前验收使用 protoc 36.2 与 protoc-gen-go/runtime v1.36.12；工具下载入口见[官方 releases](https://github.com/protocolbuffers/protobuf/releases/tag/v36.2)。

仓库示例的再生成与独立消费验证：

```sh
PROTOC=/path/to/protoc sh scripts/generate-protobuf.sh
python3 scripts/verify-protobuf.py --protoc /path/to/protoc --output /tmp/kanarpc-protobuf-new-run
```

验证脚本要求新的输出目录，保存命令、生成文件、race 结果以及双向流、保留编号重用的预期失败。脚本默认从 protoc 安装目录查找 `include`；自定义安装可传 `--include /path/to/include`。示例再生成脚本按 `go.mod` 中的 Protobuf runtime 版本安装消息插件。

## 服务注册与调用

实现生成的 `EchoServer`，并按值嵌入 `UnimplementedEchoServer`：普通方法返回响应指针与错误；流式方法通过类型化 `send` 回调发送消息。新增 RPC 后，已存在的服务实现可继续编译，尚未实现的方法返回 NotFound。调用 `RegisterEchoServer(server, implementation)` 原子注册整组方法，重复或非法路径不会留下半个服务。注册须在 Serve 前完成。

客户端在 Dial 配置中设置 `RequiredFeatures: kanarpc.FeatureProtobuf | kanarpc.FeatureServerStreaming`，先协商所需能力与帧上限，再使用 `NewEchoClient(caller)`。`caller` 可以是直连 `*Client`、`*QUICClient`、`*routing.Client` 或统一业务入口 `*ClientPipeline`；连接池配置位置为 `routing.Config.Dial.Client`。以下沿用仓库的 EchoService 定义：

```go
sdk := echo.NewEchoServiceClient(client)
response, err := sdk.Echo(ctx, &echo.EchoRequest{Text: "hello"})
// 检查 err 后读取 response.GetText()。
stream, err := sdk.Watch(ctx, &echo.EchoRequest{Count: 3})
// 检查 err 后循环 stream.Recv()，直到 io.EOF。
// 提前结束时调用 stream.Close()；Context 覆盖整个流的生命周期。
```

需要凭证、Metadata、gzip、拦截器或显式幂等重试时，先配置调用选项与 `ClientPipeline`，再把管线传给生成 SDK。完整示例与执行顺序见 [业务调用链](production-r15.md)。

已有调用入口也可直接使用：

```go
err := client.Call(ctx, echo.EchoService_Echo_FullMethodName,
    kanarpc.Protobuf(request), response)
```

`Protobuf` 只包装消息，不立即编码。客户端接纳、压力反馈和编码字节预留先执行，再进入消息的 Size/Marshal。手工注册使用 `RegisterProtobufUnary` 或 `RegisterProtobufStream`；泛型参数明确请求与响应类型。错误继续使用框架的 `Status` 与 Context 错误。

## 编码、兼容与资源契约

Protobuf 业务正文按二进制发送，控制元数据保留 JSON；格式见 [KNR2 协议](protocol-v2.md)。请求编码与方法注册不匹配时，在调用处理器前拒绝。空 Protobuf 消息可以产生零字节 DATA，仍消耗序号与流信用，保留未读流和连接池归属。

新客户端在建连阶段通过 `RequiredFeatures` 协商 Protobuf，失败时返回可由 `errors.Is(err, kanarpc.ErrCapabilities)` 匹配的错误，尚未发送业务请求。未协商的 Protobuf 调用在编码前拒绝。默认 JSON 接入不增加握手；框架不自动改用 JSON，也不重放业务请求。Protobuf Go 结构体本身不会自动切换编码。方法如需同时暴露两种编码，应注册不同路径。`.proto` 消息可复用，KanaRPC 的 wire protocol 与 gRPC 分别接入。

接收前检查帧上限并预留连接/池字节租约，二进制 OPEN 采用与 JSON OPEN 相同的两倍保守计费，DATA 与未读缓冲持续计费。Protobuf 解码递归上限为 64。消息对象、解码展开和用户代码内部临时分配需要单独评估；字节租约不等于进程 RSS 上限。

调用期间不要并发修改正在编码的消息；接收目标也需独占使用。处理器、消息扩展实现和编码函数须协作响应取消。客户端流应读取至结束或调用 Close；预算、取消、终态回执与累计故障沿用[统一使用契约](operating-notes.md)。

客户端流和双向流的 `.proto` 方法会在生成阶段报错。当前 Go SDK 覆盖普通与服务端流，跨语言客户端需要实现 KNR2 记录与生命周期契约。

生成文件包含 SDK API 1 编译期标识。schema 的二进制、JSON 和源码兼容检查、固定工具链及按版本发布方式见 [SDK 治理](sdk-r12.md)。
