# 独立仓库与开发

新版模块为 `github.com/KanaDoodle/KanaRPC`。根模块可独立构建和测试，不需要旧 KanaRPC-Go、开发 workspace 或跨目录 replace。新仓库从当前实际实现建立一个真实的初始提交，不导入旧 Git 历史，不伪造此前阶段的提交。

## 本地构建

Go 1.26.0 或更新版本：

```sh
GOWORK=off go build -mod=readonly ./...
GOWORK=off go test -count=1 ./...
GOWORK=off go test -race -count=1 ./...
```

普通测试未配置 etcd 时会跳过需要真实 etcd 的用例。完整验证要求本机可执行 `etcd`，runner 使用独占目录/loopback 端口、自行核验实例归属，不连接既有业务服务：

```sh
python3 scripts/verify-discovery.py --output /tmp/kanarpc-test-new-run
python3 scripts/verify-discovery.py --output /tmp/kanarpc-race-new-run --race
```

输出路径必须尚不存在。该 runner 对根模块执行全量测试；日志保存 pass/skip/fail、etcd membership 和进程回收状态。模型/GPU、跨机网络与多成员 etcd 不在这项验收内。

## 历史对照工具

`tools/lifecycle-compare` 是显式的独立嵌套模块，用旧项目公开 API 做行为对照，依赖其旁边的历史工作区。它被根模块 `go build ./...` / `go test ./...` 排除，不能把它的跨目录 replace 解释为新版运行依赖。

`scripts/compare-lifecycle.py` 和 `scripts/audit-origins.py` 需要显式的旧目录/本地历史 ref；普通使用者独立构建无需运行它们。历史报告记录的是当时具体工作区和源码 hash，不保证任意旧版 checkout 能复现所有对照。旧仓库、来源署名、许可和已有验证证据保留。

## 版本与证据

项目当前为实验阶段，尚未发布稳定版本。仓库可独立维护与构建，不代表旧推理适配/业务迁移已完成。后续 tag 与 SDK 接入需要自己的兼容性验收；此时不提供隐式旧协议降级或自动业务重放。

`docs/verification` 保留阶段报告、源码快照、完整 offered、失败记录与退化结果。历史 origins/manifest hash 对应当时快照；README、忽略规则或开发文档后续更新不会把历史证据重新标成当前结果。当前功能、依赖与声明边界见 [README](../README.md)、[ORIGINS](../ORIGINS.md)。

首次入库前扫描了普通文件、gzip 与嵌套 tar 内的常见凭据模式；该有限检查不能证明不存在所有秘密。原始实验 CA 是公用证书，实验私钥不保存；本地密钥、环境配置和构建产物不应提交。AGPL-3.0 正文及第三方来源说明继续保留。
