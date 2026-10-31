# 实现沿革与第三方来源

KanaRPC 的模块路径为 `github.com/KanaDoodle/KanaRPC`，自 2026-10-31 起以独立 Git 历史维护。v2 内核从空目录编写，重新设计帧布局、类型注册入口、请求状态、资源归属和关闭流程；运行内核使用本仓库实现。

## 项目沿革

维护者与编码助手此前接触过 KanaRPC-Go / KamaRPC-Go，并参考了已有使用需求与公开调用契约。旧 KanaRPC-Go 保留其上游 fork 历史、署名、AGPL 许可和验证记录。新版的独立仓库与源码记录描述实现过程；由于存在上述接触经历，这一过程不作法律意义上的 clean-room 声明。

## 实现与依赖

| 组件 | 实现方式与来源 |
| --- | --- |
| RPC 内核、测试、示例与验收脚本 | 在新版仓库编写；核心不导入旧 KanaRPC-Go 实现 |
| 编解码与网络基础 | Go 标准库的 `net`、`context`、`encoding/json` 等能力 |
| Protobuf 与代码生成 | google.golang.org/protobuf v1.36.12，BSD-3-Clause；消息由官方 protoc-gen-go 生成，本仓库实现 KNR2 二进制集成与 protoc-gen-kanarpc 插件 |
| Schema 与发布检查 | Buf CLI v1.75.0，Apache-2.0；Go 模块 ZIP 使用官方 golang.org/x/mod 工具库。构建工具在隔离目录运行，不加入 RPC 运行依赖 |
| OpenTelemetry 适配 | OTel API/SDK v1.44.0，Apache-2.0；本仓库实现生命周期、Metadata 与日志/指标适配，应用管理 exporter 和 provider |
| QUIC 传输 | quic-go v0.64.1，MIT；本仓库实现 session、请求交换、取消与关闭集成 |
| etcd 注册发现 | etcd client/api v3.7.3，Apache-2.0；本仓库实现 lease 所有权、快照新鲜度和 watch 恢复 |
| 调度与过载控制 | 本仓库实现方法加权轮转、延迟窗口 AIMD、短效压力反馈和配置回退；控制思路参考 Netflix concurrency-limits 与 Envoy adaptive concurrency |
| 通用协议机制 | 使用 request ID、信用窗口、截止时间、回执与有限接纳等已有机制 |
| 历史对照工具 | `tools/lifecycle-compare` 是独立嵌套模块，通过公开 API 调用新旧实现；根模块构建与运行不依赖该工具 |
| grpc-go 对照工具 | `tools/grpc-compare` 独立依赖 grpc-go v1.85.0（Apache-2.0），共享官方生成的 Protobuf 消息；不改变根模块运行依赖 |
| 许可 | 沿用原目录的标准 AGPL-3.0 许可证正文；第三方依赖分别遵循其许可 |

quic-go、etcd 和 Protobuf runtime 源码通过 Go 模块依赖获取，未复制入本仓库。`examples/echo/*.pb.go` 的消息部分来自 schema 的工具生成，RPC 绑定由本仓库插件生成。项目的工程实现与集成工作建立在这些依赖和通用机制之上。

## 来源记录

`scripts/audit-origins.py` 记录本地上游 ref、旧版 HEAD、源码 SHA-256、整文件相同情况及核心依赖图，供维护者核查具体快照。来源记录用于工程溯源；法律归属和完整相似性分析需另行评估。

后续新增或移植组件应同步记录来源与适用许可。版本状态以仓库标签和对应源码为准，阶段验收见 [验证索引](docs/verification/README.md)。
