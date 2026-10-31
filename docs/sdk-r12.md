# 协议协商、schema 检查与 SDK 发布

## 实施顺序

| 阶段 | 结合现有代码的改动 | 必要验证 |
| --- | --- | --- |
| R12A 能力协商 | 在现有记录层增加建连 HELLO/WELCOME；默认 JSON 不增加握手，Protobuf 在编码前检查协商结果；QUIC 使用会话控制流 | 未协商消息不进入编码；旧节点、能力不足、超时与取消；TCP/TLS/QUIC、连接池和关闭回归 |
| R12B schema 兼容 | 使用官方 Buf 的 FILE 规则和固定 descriptor 基线；对已发布 Git ref 也可检查 | 新增字段通过；编号复用、类型/presence、方法及流方向破坏明确失败 |
| R12C SDK 发布 | 生成代码加入编译期 API 标识；插件版本可查询；生成检查、发布前校验与 GitHub Actions | 独立消费模块、过期生成文件拒绝、版本及发布包本地演练；远端执行状态单独记录 |

三项工作沿用既有预算、取消、终态回执、连接退休和累计故障契约。CampusTrace 与简历保持本轮工作范围之外。

## 建连能力协商

Protobuf 接入在创建连接时声明所需能力：

```go
cfg := kanarpc.ClientConfig{
    RequiredFeatures: kanarpc.FeatureProtobuf | kanarpc.FeatureServerStreaming,
}
client, err := kanarpc.Dial(ctx, address, cfg)
// 检查 err 后创建生成的 SDK；client.Capabilities() 返回协商结果。
```

TLS/QUIC 使用相同 ClientConfig；发现连接池配置 `routing.Config.Dial.Client`。握手只执行一次，TCP/TLS 在业务连接上完成，QUIC 使用父会话的独立控制流。它增加一次建连往返，后续每次 RPC 使用缓存结果。

两端采用较小的帧上限。握手不占处理器或请求队列名额，读写使用控制字节预算并响应 Context。所需能力不足、旧节点不识别握手、格式非法或等待超时都会在业务发送前返回；`errors.Is(err, ErrCapabilities)` 可识别能力错误，其内部错误保留取消与截止时间身份。未协商的 Protobuf 调用不会进入用户编码器。

默认 JSON 配置不发握手，继续接入旧节点。协商不触发自动降级、重连或业务重放，也不替代 TLS/mTLS 与方法授权。能力声明说明协议功能可用，具体方法是否存在仍由服务注册决定。记录布局见 [KNR2 协议](protocol-v2.md)。

## 工具与 schema 检查

`tools/versions.json` 固定 Go、Protobuf、Buf、protoc 与 etcd 分发版本。安装器先校验官方 release 的 SHA-256，再把工具放入新建的自有目录，不替换系统安装：

```sh
python3 scripts/install-tools.py --output /tmp/kanarpc-build-tools
export PATH="/tmp/kanarpc-build-tools/bin:$PATH"
python3 scripts/check-schema.py
python3 scripts/verify-schema.py --output /tmp/kanarpc-schema-new-run
```

安装器提供 Linux amd64 与 macOS arm64 分发。其他平台可按官方说明安装同版本工具，并通过 `--buf/--protoc` 指定路径。输出目录均须尚不存在。

兼容检查分三个维度执行：

| 规则 | 保护内容 | 实际验证的例子 |
| --- | --- | --- |
| WIRE | 二进制消息与 RPC 契约 | 字段类型、RPC 流方向变化被拒绝 |
| WIRE_JSON | 二进制与 JSON 名称/表示 | 字段改名即使二进制仍兼容，也被拒绝 |
| FILE | 生成源码 API | presence、Go 包路径变化，以及删除已保留字段也会被拒绝 |

规则由 [Buf 官方工具](https://buf.build/docs/breaking/rules/)维护。`api/schema.binpb` 是本地开发基线，生成 Go 代码不会覆盖它。CI 从 PR 目标提交、发布前从上一个版本 tag 的 schema 源码生成比较镜像，防止同一改动同时更新基线而绕过检查。首次引入 schema 时会明确记录 initial_schema。

新增 RPC 通过兼容检查；服务实现按值嵌入生成的 `Unimplemented<Service>Server`，新增方法默认返回 NotFound。生成文件包含 SDK API 1 编译期标识，配套插件支持 `--version`。消息字段还需要遵守[官方演进规则](https://protobuf.dev/programming-guides/proto3/)；兼容规则不能判断业务语义是否保持一致。

## 发布候选与版本接入

当前发布入口采用 v0 语义版本，公开可用版本以仓库 tag 为准。本轮进行本地候选构建和安装验收，没有创建公开 tag。

```sh
python3 scripts/release-baseline.py --release v0.1.0-rc.1 --output /tmp/kanarpc-release-base
python3 scripts/prepare-release.py \
  --version v0.1.0-rc.1 \
  --against /tmp/kanarpc-release-base/schema.binpb \
  --output /tmp/kanarpc-release-candidate
```

候选流程执行只读构建、vet、schema 检查、生成一致性、独立 Opaque SDK、真实单成员 etcd 全量 race、版本化 Go 模块验收，再生成模块 ZIP、Linux amd64/macOS arm64 插件、descriptor 与 SHA256SUMS。源码在检查期间发生变化会使流程失败；`--require-clean` 额外要求已提交的干净 checkout，供发布工作流使用。

模块验收通过本地 Go 文件代理安装候选：消费模块不使用 replace，单独缓存模块，实际运行普通调用、混合 JSON、流式取消与 drain；还执行带版本的 `go install` 并核对插件版本。它验证分发包可用，不表示该候选已经上传公共 Go proxy。

`docs/verification/go.mod` 将历史验证目录划为独立模块边界。依据 [Go 模块 ZIP 规则](https://go.dev/ref/mod#zip-files)，根 SDK 下载包不包含该目录；仓库中的原始报告与失败证据保留。

## GitHub 工作流

- `ci.yml` 在 PR/main push 上执行 Go 1.26.10 与 1.27.3 检查，保存 schema、真实 etcd、SDK 和模块验收日志。
- `release.yml` 在维护者推送 v0 tag 后重新验证干净提交，构建带版本制品并创建 GitHub Release 草稿。tag 被推送后即可被 Go 按版本获取，Release 草稿不会隐藏该 tag。
- Actions 使用固定 commit；构建工具使用固定版本与校验和。发布权限限定在 release job，PR 检查使用只读权限。

发布前先核对候选与源码，再创建版本 tag；已经发布的 tag 保持不可变。远端工作流运行、公开安装和草稿确认分别在对应环境验收。[本轮验证报告](verification/sdk-r12/report.md)记录本地实测与尚未执行的远端步骤。
