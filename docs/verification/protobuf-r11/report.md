# R11 · Protobuf、类型化 SDK 与组件升级

2026-11-01。基线提交 `3feefd9735415b5aade1f775f6c69a1f55b968ce`。本轮修改 KanaRPC；CampusTrace 与简历未修改。

Protobuf 二进制调用与 `.proto` 生成的 Go SDK 已接入普通和服务端流式 RPC，覆盖 TCP/TLS/QUIC、发现连接池、编码前接纳、取消和关闭。组件更新采用 Protobuf v1.36.12、etcd client/api/pkg v3.7.3、x/net v0.62.0；Go 最低版本调整为 1.26.10，工具链固定为 1.27.3。

## 实施与阶段验证

| 阶段 | 修改与验证 | 结果 |
| --- | --- | --- |
| 基线 | 修改前 `GOWORK=off go test ./...` | 通过，原始结果归档 |
| 业务编码 | 保留 JSON 记录，新增 OPEN_ENCODED / DATA_ENCODED；在现有接纳与资源路径中选择编码 | TCP/TLS/QUIC 混合调用、未知字段往返、空消息、编码不匹配、非法负载、取消与未读缓冲测试通过 |
| 类型化 SDK | 官方 protogen 生成方法常量、客户端、流接收器与原子服务注册 | 真实 protoc 生成并编译；重复/非法方法注册不留下部分服务 |
| 连接池与资源 | 增加公共 ServiceCaller/OpenStream 接口，保留 routing 流引用 | 三种传输的节点移除、零字节未读结果与退休账户回收通过；累计创建/释放数量相等，最终使用字节为零 |
| 独立消费模块 | 从 Editions 2024 定义生成 Opaque API 消息，并引用外部消息类型 | 独立模块 race 运行普通、流式、空值 presence、取消与 drain；重新生成文件与仓库内文件逐字节一致 |
| 组件升级 | 核对官方稳定版本、依赖图和安全公告，迁移 Watch Header | 复现迁移故障后修复；真实 etcd 与完整回归通过 |

实现步骤与 API 见 [Protobuf 使用指南](../../protobuf.md)；组件保留/升级取舍见[组件评估](../../components-20261101.md)。

## 故障复现与修复

### etcd 3.7 Watch Header 指针化

升级前客户端为 v3.6.7。升级到 v3.7.3 后，原测试中按值构造的 `WatchResponse.Header` 先出现三处编译错误。更新测试构造后，在现有新鲜度测试中传入 `WatchResponse{}`，复现 `Resolver.apply` 解引用空 Header 的 panic。

```sh
go test ./discovery -run '^TestCreatedAndHistoricalEventsCannotRenewFreshness$' -count=1
```

根因是上游 Protobuf 迁移把 Header 改为指针，本地代码仍直接读取字段。修改 Watch 应用逻辑，使用 `GetClusterId/GetRevision`；空 Header 不再导致 panic，也不能为过期快照续期。原客户端已对实际 Watch 响应规范化 Header，因此该复现证明的是迁移适配缺口，并未证明真实 etcd 会发出导致该 panic 的响应。

修复后上述用例通过；Created、历史事件、同 revision 事务、compaction 重建、lease 所有权和网络断连新鲜度回归均通过。失败与修复日志分别保存在归档中的 `etcd-pointer-reproduction.txt`、`etcd-pointer-fixed.txt`。

### 新编码必须遵守既有接纳与归属

[codec_test.go](../../../codec_test.go) 使用带计数器的 `ProtoReflect`，实际验证已取消、名额耗尽、反馈拒绝、编码容量等待超时和已关闭客户端都不进入消息编码。nil、typed nil 和超长消息返回错误后，准备名额与编码字节归零。

接收端验证错误编码在处理器执行前拒绝；畸形二进制正文返回 InvalidArgument。流式用例验证 CANCEL 到达远端、空 DATA 仍消耗信用、未读响应继续计费、Close 后回收。[生成 SDK 用例](../../../protobuf_sdk_test.go)还验证显式提供的空字符串 presence，以及零字节未读结果在 endpoint 退休期间仍保护连接引用。

这些是新能力的接纳与生命周期验收，不作为旧 JSON 路径已经存在对应缺陷的结论。

## 实际链路与回归

环境为 macOS 27.0.1 / Apple M5 / darwin-arm64。主要运行使用 Go 1.27.3，另在 Go 1.26.10 运行普通测试验证最低版本。完整版本见 [environment.json](environment.json)。

| 检查 | 命令 / 环境 | 结果 |
| --- | --- | --- |
| 普通全量测试 | 自有单成员 etcd 3.6.16，`scripts/verify-discovery.py` | 通过；包含子用例共 362 项，零 skip/fail |
| race 全量测试 | 自有单成员 etcd 3.7.3，runner 加 `--race` | 通过；362 项，零 skip/fail |
| 最低 Go 版本 | `GOTOOLCHAIN=go1.26.10 go test ./... -count=1 -timeout=180s` | 通过；未配置 etcd 的用例由上面真实实例运行覆盖 |
| 最终 Protobuf race | `go test . -race -run 'TestProtobuf\|TestGeneratedProtobuf' -count=1` | 通过，含空值 presence 的最终输入 |
| 生成 SDK / Opaque | `python3 scripts/verify-protobuf.py --protoc /path/to/protoc --output /tmp/new-run` | 通过；包含独立模块 race 和生成一致性 |
| 可运行示例 | `go run ./cmd/protobuf-demo` | 普通、JSON 混合、三段流、远端取消通过；drain Quiescent=true，累计交付故障为零 |
| fuzz | `go test . -run '^$' -fuzz '^FuzzEncodedOpen$' -fuzztime=8s -parallel=2` | 109,993 次执行，无失败；[原始日志](fuzz.txt) |
| 静态检查与构建 | `go vet ./...`、`go build -mod=readonly ./...`、脚本语法检查 | 通过 |

etcd 升级前还运行了旧客户端 + 系统 etcd 3.7.1 的真实基线。各实例使用独占数据目录与 loopback 端口，完成后核验自有进程已回收；系统既有服务和二进制未替换。

额外构建基线与新版两个进程，双向验证 JSON 普通/流式调用。旧服务端收到合法形状的 OPEN_ENCODED 后 reset 连接，处理器接纳数保持为两次 JSON 调用，没有执行该 Protobuf 请求。兼容结论是 JSON 互通、新记录明确拒绝；没有隐式降级或重放。

## 编码对照与负结果

`BenchmarkPayloadEncoding` 对同一 EchoRequest 数据比较既有 `encoding/json` 与二进制 Protobuf，Protobuf 编码包含 Size 预检查。每轮每组运行 200ms、重复三次；不包含网络、方法调度、记录元数据或 24 字节帧头。

| 输入 | JSON 正文字节 | Protobuf 正文字节 | JSON / Protobuf 编码分配字节 |
| --- | ---: | ---: | ---: |
| 空消息 | 2 | 0 | 8 / 0 |
| 短文本和计数 | 30 | 13 | 32 / 16 |
| 3072 字节文本 | 3083 | 3075 | 3201 / 3200 |
| 256 个整数 | 926 | 387 | 1024 / 416 |

长文本正文只减少 8 字节；短文本解码中 JSON 为 1 次分配，Protobuf 为 2 次，分配量从 128 增至 144 B/op。首轮长文本编码中位数为 JSON 1555 ns/op、Protobuf 1669 ns/op，Protobuf 慢约 7.3%；后一次运行方向反转，说明该时间差不稳定。本机为非独占环境，不据此宣称通用时延或吞吐提升。

这组结果说明字段结构会影响编码收益，且正文缩小不等于完整请求或进程内存同比下降。保留[首轮](codec-bench-initial.txt)、[最终原始结果](codec-bench.txt)和[最终中位数](codec-bench-summary.json)；未选取有利结果覆盖负结果。

## 依赖检查与工具失败

首次从 GitHub releases 取到 govulncheck v1.1.4，运行时因旧分析器不兼容 Go 1.27 类型系统而 panic；原始输出保留。随后通过 Go 模块查询确认 v1.9.0，安装该版本并重新执行。

旧依赖与 Go 1.27.1 下，当前扫描器报告 15 项符号可达告警，涉及旧 gRPC、x/net 和标准库。升级后扫描无符号级或包级告警；剩余一条模块级 [GO-2026-5932](https://pkg.go.dev/vuln/GO-2026-5932)，对应未导入的 `x/crypto/openpgp`，上游没有修复版本。该扫描是已知漏洞与静态可达性检查，不是攻击利用复现。详细输出见 [govulncheck.txt](govulncheck.txt)。

单次 wire 兼容验证脚本还保留了两次工具失败：字符串引用错误导致脚本未启动；随后脚本只接受 EOF，但旧服务实际返回 TCP reset。修正验证器以接受这两种连接拒绝方式后，双向 JSON 和未执行新记录的检查通过。两者未改动 RPC 产品代码。

## 使用与验证边界

Protobuf 接入要求两端支持新记录类型；普通与服务端流式方法可用，客户端流/双向流在生成时拒绝。保留字段编号重用也由 protoc 明确拒绝，预期失败输出包含在归档中。当前不是 gRPC wire 接入，跨语言 SDK 需实现 KNR2。

字节租约覆盖框架计费范围，消息对象、解码展开和编解码内部临时分配按使用契约单独评估；取消依赖用户函数协作。真实双机、WAN、多成员 etcd、小时/天级运行和 GPU/vLLM 性能本轮未验证。框架能力与部署约束见[接入指南](../../protobuf.md)和[统一使用契约](../../operating-notes.md)。

## 证据与复现

- [manifest.json](manifest.json)：基线提交、源码与产物 SHA-256、检查结果。
- [source.tar.gz](source.tar.gz)：本轮根模块源码、生成文件、脚本、Web 资源和当前文档；不包含历史验证目录与嵌套对照工具。
- [runs.tar.gz](runs.tar.gz)：etcd 普通/race 收据与日志、独立消费模块、迁移失败、扫描前后结果与兼容检查。
- [dependency-releases.json](dependency-releases.json)：核查时的官方 release 地址与发布时间。

复现先使用该源码快照及其固定依赖；protoc 的生成验收版本为 36.2。常用命令列于上表，etcd runner 参数见 `scripts/verify-discovery.py --help`，SDK 参数见 `scripts/verify-protobuf.py --help`。历史记录保留原始环境与路径，不覆盖其他阶段结果。
