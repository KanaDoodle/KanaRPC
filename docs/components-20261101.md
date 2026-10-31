# 组件与设计评估 · 2026-11-01

本轮先完成 Protobuf 二进制调用和 SDK，再核对官方稳定版本、迁移说明和当前源码。升级目标是明确接口契约、更新受维护依赖并保留已验证的生命周期行为。

## 已落地更新

| 组件 | 调整 | 依据与实际作用 |
| --- | --- | --- |
| Protobuf runtime / 消息插件 | v1.36.5 → v1.36.12，插件版本从模块依赖读取 | 使用[官方最新稳定版](https://github.com/protocolbuffers/protobuf-go/releases/tag/v1.36.12)，让 runtime 与生成代码使用同一版本；独立应用实际编译运行 |
| 接口定义与 Go SDK | 新增 protoc-gen-kanarpc，验证 protoc 36.2、proto3 与 Editions 2024 Opaque API | 从 schema 生成方法路径、请求/响应类型、流接收器与原子注册；插件不依赖生成消息的公开字段，符合[官方 Opaque API 演进方向](https://protobuf.dev/reference/go/opaque-migration/) |
| etcd client/api/pkg | v3.6.7 → v3.7.3 | [3.7 迁移到官方 Protobuf runtime](https://github.com/etcd-io/etcd/blob/main/CHANGELOG/CHANGELOG-3.7.md)，模块图不再包含 gogo/protobuf；同时带入较新的 gRPC、x/net 等依赖 |
| x/net | v0.56.0 → v0.62.0 | 更新 HTTP/2 依赖，并覆盖扫描记录中的 [GO-2026-6866](https://pkg.go.dev/vuln/GO-2026-6866)、[GO-2026-6860](https://pkg.go.dev/vuln/GO-2026-6860) 修复 |
| Watch Header 适配 | 指针化响应改用 nil-safe getter；更新测试构造 | 空 Header 的迁移用例先复现 panic，再修复；保留 Created/历史事件不能续期快照的新鲜度契约 |
| Go 安全补丁 | `go 1.26.10`，`toolchain go1.27.3` | [官方支持分支补丁](https://go.dev/doc/devel/release)；Go 1.27.1 下的扫描告警涉及标准库，升级工具链后重新扫描与回归 |

etcd 升级的是应用依赖；系统已安装的 etcd 和 Go 未被替换。验收使用临时目录下载的官方 etcd 3.7.3，以独立数据目录和端口启动单成员实例，结束后核验进程回收。集群服务端升级仍遵循[官方 3.6 → 3.7 指南](https://etcd.io/docs/v3.7/upgrades/upgrade_3_7/)。

## 继续采用的设计

| 设计 | 代码依据与取舍 |
| --- | --- |
| quic-go v0.64.1 | 核查时已是[最新稳定版](https://github.com/quic-go/quic-go/releases/tag/v0.64.1)。保留 `TryWriteAll` 有界批写与 `WriteWithLimit` 回退，deadline/reset 保护已有故障复现；上游更新不等于可以删除本地保护 |
| TCP/TLS 与 QUIC 并存 | TCP/TLS 复用记录流，QUIC 每请求独立流；两者满足不同部署条件，共用预算、取消和回执契约 |
| 显式处理器注册 | 泛型 JSON 与生成 Protobuf 注册在 Serve 前冻结；避免运行时按方法名反射分派，错误尽早暴露 |
| Context、有限调度与字节租约 | 使用 `WithCancelCause`、`AfterFunc` 和有归属的清理；取消、终态与未读缓冲的生命周期需要完整保留，不能用“更短的异步封装”替代 |
| etcd Lease/Watch + revision/cluster/新鲜度 | 服务发现仍需要断连过期、压缩重建和所有权校验；不能把能连上 etcd 当成快照可靠 |
| 连接池轮询发现快照 | 单一有界循环；调用接纳时也校验快照。订阅通知可以降低刷新等待，但目前没有测量证明该部分是瓶颈，继续保留现有生命周期边界 |
| JSON 默认、Protobuf 按方法选用 | 保留无 schema 调用体验；有稳定契约的服务可选择二进制 SDK。编码方式不承担业务重试或幂等职责 |

## 后续最有价值的设计演进

下列三项已在后续 [R12 SDK 治理](sdk-r12.md)中落地，保留这里的原评估依据。

1. **协议能力协商。** 当前 Protobuf 通过新增记录类型显式启用，接入配置要求两端升级。混合版本节点部署前，可增加有界能力握手，在业务发送前识别编码能力；保持不自动重放业务请求。
2. **schema 兼容检查。** 本轮验证了未知字段往返、optional presence 和保留编号冲突。下一步可在发布流程对比前后 descriptor，拦截编号复用、类型变化和服务签名破坏，减少跨版本集成错误。
3. **稳定的 SDK 发布链路。** 生成器与 runtime 随同一模块版本分发；后续通过版本 tag、生成差异检查和兼容矩阵建立发布入口，避免应用自行组合未经验证的插件版本。

这些演进围绕接入与版本管理展开；当前请求调度、资源归属和关闭内核已有明确契约，本轮保持其实现结构。

## 安全扫描与验证结果

govulncheck v1.9.0 在升级前的依赖与 Go 1.27.1 下报告 15 项符号可达告警；它们是静态可达性结果，不是本项目的攻击复现。更新依赖与工具链后，未报告可达漏洞；仍存在 1 条仅模块级、当前未命中导入/调用路径的 openpgp 记录，详情保留在验证产物中。`github.com/golang/protobuf` 兼容包仍由上游依赖引入，本项目的新增业务编码与生成器统一使用 `google.golang.org/protobuf`。

普通、race、真实单成员 etcd、独立 SDK、fuzz 与本地编码对照的命令、失败记录和环境见 [R11 验证报告](verification/protobuf-r11/report.md)。真实双机、WAN、多成员 etcd 和真实模型推理的环境范围沿用[使用约束](operating-notes.md)。
