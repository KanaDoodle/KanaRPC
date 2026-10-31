# 开发与 SDK 接入

模块 `github.com/KanaDoodle/KanaRPC` 可独立构建和测试，根模块的运行依赖由 `go.mod` 管理。源码沿革与第三方许可见 [ORIGINS](../ORIGINS.md)。

## 本地构建

需要 Go 1.26.10+；仓库工具链固定为 Go 1.27.3。使用其他受支持分支时请安装该分支的最新安全补丁：

```sh
GOWORK=off go build -mod=readonly ./...
GOWORK=off go test -count=1 ./...
GOWORK=off go test -race -count=1 ./...
```

普通测试在未配置 etcd 时跳过对应集成用例。完整验证要求本机可执行 `etcd`；runner 创建独占数据目录与 loopback 端口，核验实例归属，并在结束时回收自有进程：

```sh
python3 scripts/verify-discovery.py --output /tmp/kanarpc-test-new-run
python3 scripts/verify-discovery.py --output /tmp/kanarpc-race-new-run --race
```

输出目录应尚不存在。runner 对根模块执行全量测试，保存 pass/skip/fail、etcd membership 与进程回收状态。

## 消费模块

目前使用经审查提交的 pseudo-version 固定 SDK 版本；稳定发布以仓库 tag 为准。在应用自己的 Go 模块中导入根包，以及按需使用的 `discovery` 和 `routing`。本地联调可在消费模块中配置 `replace` 指向 KanaRPC checkout，并用 `GOWORK=off` 验证独立构建。

可编译的公共 API 示例见 [routing/example_test.go](../routing/example_test.go) 和 [drain_example_test.go](../drain_example_test.go)；发现、TLS 配置、流与退出顺序见 [连接管理](connections-r8c.md)。新旧协议使用独立端点，应用在接入时明确配置。

Protobuf 服务使用 `.proto` 生成类型化客户端和注册入口，直连与连接池共用 `ServiceCaller` 接口。完整命令、编译器版本和兼容约束见 [Protobuf 与 Go SDK](protobuf.md)。运行已生成的示例无需 protoc；修改 schema 时再安装生成工具。

固定工具分发、schema 检查、版本化 Go 模块验收与发布候选流程见 [SDK 治理](sdk-r12.md)。历史证据位于独立的 `docs/verification` 模块边界中：GitHub 仓库保留全部记录，消费根模块时不下载该目录。

业务接入建议通过 `ClientPipeline` 统一凭证、限流、显式幂等重试和观测。压缩、健康发布及完整调用顺序见 [业务调用链](production-r15.md)。可用 `go run ./cmd/production-demo` 运行真实本机链路，`cmd/kanarpc` 提供 JSON/Protobuf 调试入口。

## 按需验证

| 入口 | 覆盖范围 |
| --- | --- |
| `scripts/verify-production.py` | 真实单成员 etcd，TCP/TLS/QUIC 独立进程，凭证、压缩、SDK/CLI、追踪关联、取消与 drain；支持 `--race` |
| `scripts/verify-core.py` | 独立进程 TCP、普通与流式调用、取消和 SIGTERM 关闭 |
| `scripts/verify-transports.py` | TCP/TLS/QUIC 调用与内部负载对照 |
| `scripts/verify-governance.py` | 固定并发、公平调度、自适应、反馈与回退策略 |
| `scripts/verify-resources.py` | 编码前接纳、混合流负载与周期资源观察 |
| `scripts/verify-routing.py`、`scripts/verify-sdk.py` | 连接管理对照与独立消费模块 |
| `scripts/verify-protobuf.py` | 可重复生成、独立 Editions/Opaque SDK、真实 TCP 与预期编译失败 |
| `scripts/verify-network.py` | 自有 Linux VM 容器内的内核 netem，需要基线源码 |
| `scripts/verify-pool.py` | 池预算、退休对象、未读流和饱和场景 |
| `scripts/compare-quic-write.py` | QUIC 发送策略与多 payload 内部基线 |
| `scripts/verify-showcase.py` | HTTP + QUIC 控制台操作、取消、关闭与历史故障 |

运行参数和资源归属要求见各组件文档与脚本 `--help`。性能对照应冻结源码后串行执行；race 运行用于正确性检查。环境与测量口径集中在 [使用约束](operating-notes.md)，原始结果见 [验证索引](verification/README.md)。

## 历史对照与证据维护

`tools/lifecycle-compare` 是独立嵌套模块，通过公开 API 对照新旧行为，使用其旁边的历史工作区。根模块的 `go build ./...` 与 `go test ./...` 不包含该工具。`scripts/compare-lifecycle.py` 和 `scripts/audit-origins.py` 需要显式的旧目录或本地历史 ref。

`docs/verification` 按阶段保存报告、源码快照、完整 offered、失败和退化结果。manifest 与 hash 对应当时的源码及工具版本；新增验证应保存为独立记录。历史复现需要使用报告注明的 checkout、环境和依赖版本。

## 提交约定

本地密钥、环境配置和构建产物保留在仓库外。首次入库时已对普通文件、gzip 和嵌套 tar 执行常见凭据模式扫描；后续提交继续检查新增文件。实验 CA 公用证书可以保存，私钥通过运行时生成或本地配置提供。保留 AGPL-3.0 正文及第三方来源说明。
