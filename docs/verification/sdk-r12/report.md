# R12 · 协议协商、schema 与 SDK 发布候选

2026-11-01。本轮在 R11 工作区上继续实现，Git 基线仍为 `3feefd9735415b5aade1f775f6c69a1f55b968ce`；最终候选明确记录为 dirty checkout 的本地验收，没有创建 tag、推送或公共 Release。CampusTrace 与简历未修改。

## 交付结果

- TCP/TLS/QUIC 在发布连接前协商所需能力和双向帧上限；默认 JSON 不发握手。Protobuf 未协商时在编码前拒绝，失败后不自动降级或重放。
- Buf 分别检查 WIRE、WIRE_JSON 和 FILE 兼容性。PR/发布基线从目标提交或上一版本 tag 的 schema 源码构建。
- 生成 SDK 带 API 1 编译期标识与可嵌入的默认服务实现；新增 RPC 后已有实现继续编译，未实现方法返回 NotFound。
- 构建工具固定版本与官方 SHA-256；本地发布入口执行检查、按版本模块消费和插件打包。GitHub CI/release 工作流通过 actionlint，但尚未远端执行。

使用说明与阶段步骤见 [SDK 治理](../../sdk-r12.md)。最终源码与结果见 [manifest](manifest.json)、[源码快照](source.tar.gz)和[原始记录](runs.tar.gz)。

## 实际复现与修复

### 未协商的 Protobuf 进入编码与发送

在没有能力协商的客户端上发起 Protobuf，计数型 ProtoReflect 记录 3 次调用，客户端 Started=1，最终因没有响应超时。这表明发现能力不匹配前，已经进入消息编码和发送路径。

```sh
go test . -run '^TestProtobufRequiresNegotiationBeforeEncoding$' -count=1
```

增加 `RequiredFeatures`、HELLO/WELCOME 与编码前检查后，同一用例返回 ErrCapabilities，计数与 Started 均为零。握手不占业务处理器名额，使用有界控制预算，取消/截止时间仍可通过 errors.Is 匹配。

真实基线进程还验证了旧 KNR2 节点：握手在业务前被拒绝；随后新建默认 JSON 连接，普通与流式调用通过，旧服务端接纳数恰好为这两次业务调用。[进程结果](legacy.txt)未使用自动降级或业务重放。

### QUIC 新流未在分配前采用协商上限

第一轮功能测试通过后，复查发现 QUIC 新请求流最初仍使用服务端配置的帧上限，到解析 OPEN 时才应用父会话上限。

复现配置：服务端 4096 字节，客户端 1024 字节，协商结果为 1024。原生流只发送声明 2048 字节正文的帧头，不发送正文。修复前服务端等待正文并预留 4096 字节，客户端读等待超时；业务接纳数为零，但分配前限制已经失效。

```sh
go test . -race -run '^TestNegotiatedQUICFrameLimitPrecedesAllocation$' -count=1
```

修复在创建 serverConn、启动读取 goroutine 前，从父会话复制选定帧上限。原用例现在直接 reset 请求流，接收类 Peak 保持零；Hello 的控制字节单独计费。请求、响应和 END 均继续使用协商后的上限，累计故障与关闭契约保持一致。失败与修复日志在 `runs.tar.gz` 中分别为 `quic-limit-before.txt`、`quic-limit-fixed-race.txt`。

## schema 对照

使用官方 Buf v1.75.0，对真实 descriptor 执行 11 组变更，结果如下：

| 变更 | WIRE | WIRE_JSON | FILE |
| --- | --- | --- | --- |
| 不变、新增字段、新增 RPC | 通过 | 通过 | 通过 |
| 字段改名/同编号换名称 | 通过 | 拒绝 | 拒绝 |
| string 改 int64 | 拒绝 | 拒绝 | 拒绝 |
| 移除 optional presence | 通过 | 通过 | 拒绝 |
| 修改 go_package | 通过 | 通过 | 拒绝 |
| 删除字段并保留原名称/编号 | 通过 | 通过 | 拒绝 |
| 修改 RPC 流方向 | 拒绝 | 拒绝 | 拒绝 |
| 重用 reserved 编号 | 编译拒绝 | 编译拒绝 | 编译拒绝 |

这一区分保留了“二进制仍兼容，但 JSON 或生成源码已破坏”的结果。规则不推断业务语义是否保持一致。完整结果见 [schema-results.json](schema-results.json)。

另建独立 Git 仓库复现基线覆盖：同时改 schema 和当前 descriptor，直接自比较会通过；从目标 Git 提交构建参考镜像后，破坏被拒绝。也验证了 beta/rc/正式版本排序与上一 tag 选择。[可信基线记录](trusted-baseline.json)保留了对照结果。

## SDK 与发布验收

独立消费模块实际执行 protoc 36.2、Protobuf Go v1.36.12，覆盖 Editions 2024 Opaque API、跨包消息、普通/流式调用、取消和 drain。随后给 schema 新增 Later RPC，保持原服务实现不变；嵌入的默认实现使它继续编译，新方法返回 NotFound。重新生成的仓库文件逐字节一致。

负例还验证：人为修改生成文件会被一致性检查拒绝；移除 runtime 的 API 1 标识，生成代码编译失败。非法版本与要求干净提交时的 dirty checkout 也明确拒绝。[负例记录](sdk-negative.json)没有被当作产品成功用例。

最终候选经本地 Go 文件代理消费，没有 replace，使用隔离模块缓存；公共依赖可从只读、已校验的下载缓存获取，候选始终优先来自自有代理。实际执行普通 RPC、JSON 混合、三段流、远端取消与 drain；还执行带版本 go install，插件报告 `v0.1.0-rc.1 sdk-api=1`。[运行输出](demo.txt)与[模块记录](module-result.json)已保存。

历史验证目录约 146 MiB，通过 `docs/verification/go.mod` 的模块边界保留在仓库、排除出根模块下载。测试在暂存目录中放入真实证据文件：有边界时 Go 官方 ZIP 工具将其排除，移除边界后文件重新进入 ZIP。[正反对照](module-boundary.json)验证的是 Go 模块规则，而非单纯依赖手工过滤。最终候选 ZIP 为 **383,451 字节**，不是网络吞吐或推理性能指标。

候选制品另包含 Linux amd64 与 macOS arm64 插件、schema.binpb、SHA256SUMS，以及项目与第三方许可。macOS 插件已实际执行；Linux 插件完成交叉编译和 ELF 检查，尚未运行。本地制品目录为 `/tmp/kanarpc-r12.6tSA1M/release-accepted`，校验和另存于 [candidate-SHA256SUMS](candidate-SHA256SUMS)。

## 最终检查

| 检查 | 结果 |
| --- | --- |
| 修改前全量基线 | 通过；未配置 etcd 的集成用例由后续自有实例覆盖 |
| Go 1.26.10 普通测试 | 通过 |
| Go 1.27.3 全量 race + 真实 etcd 3.7.3 | 含子用例 373 项通过，零 fail/skip，自有进程已回收 |
| 独立 SDK + 新增 RPC 后的实现兼容 | race 通过 |
| 构建、vet、生成一致性、schema 检查 | 通过 |
| govulncheck v1.9.0 | 无包级/符号可达告警；仍有 1 条未命中当前导入/调用路径的模块记录，见[扫描输出](govulncheck.txt) |
| Wire 预算 fuzz | 5 秒、1,150,941 次执行，无失败 |
| GitHub 工作流 | actionlint v1.7.12 通过；远端未运行 |
| 发布候选 | 本地校验和打包通过，检查期间源码哈希保持一致 |

环境与依赖见 [environment.json](environment.json)，全量 race 收据见 [race-result.json](race-result.json)。

## 保留的失败与成本

首轮生成器把 API 常量放在 package 声明前，protoc 拒绝生成；调整位置后重新生成、编译和运行。池账户用例初版错误地期待 Dial 自动移除创建者账户，后来按既有契约显式 Release，验证失败 Dial 留下的账户可安全释放；没有修改账户所有权规则。

一次候选在公共 Go proxy 下载 Protobuf ZIP 时发生 EOF，发布检查保持失败，日志完整保留。验收脚本随后增加只读依赖下载缓存代理，并在新的候选目录重跑通过；没有重试或重放 RPC 业务。

显式能力协商增加一次建连往返，HELLO/WELCOME 各为 24 字节头加 32 字节正文，共 112 字节框架记录，不含传输封装。默认 JSON 不增加握手。本轮没有吞吐对照，不宣称请求吞吐保持不变或获得提升。

## 范围

这些结果来自本机真实 socket、独立单成员 etcd、本地 Git/Go 模块代理和实际工具链。GitHub-hosted CI、公共 tag/Go proxy 安装、Linux 插件执行、真实双机/WAN、多成员 etcd 及 GPU/vLLM 性能尚未验收。Release 草稿不会隐藏已推送的 tag，发布动作应按 [SDK 治理流程](../../sdk-r12.md)单独执行。

R11 与更早报告保持原始内容。当前源码、原始失败与成功收据分别归档；源码校验对应本轮最终状态，不回填历史结果。
