# R15：业务接入与 RPC 调用链验证

实现与使用方式见 [完整调用链](../../production-r15.md)。本轮按“调用上下文 → 凭证与策略 → 路由 → 编码/传输 → 鉴权/执行 → 响应与观测 → 退休/关闭”组织功能，CampusTrace 未修改。

## 实施与验证顺序

| 阶段 | 实现 | 验证重点 |
| --- | --- | --- |
| 业务上下文 | 有界 Metadata/trailer、生命周期 hook、凭证与可信身份、Peer、扩展状态 | 编码前拒绝、hook 顺序、未鉴权不进入业务方法、旧格式行为 |
| 负载与资源 | 协商调用选项、JSON/Protobuf 按消息 gzip、解压与发送预留 | 明文/线上长度、CRC、尾随 member、取消隔离、零计费尾部 |
| 调用策略 | 统一 ClientPipeline、总 deadline、按尝试 QPS、显式幂等重试 | 非声明方法/流不重放、失败响应隔离、重试不绕额度、panic 释放名额 |
| 实例与连接 | RR/平滑 WRR/P2C、endpoint 熔断/半开/代次、keepalive、空闲退休 | 调权不重连、单探测、迟到结果、协议负载与未读租约分离 |
| 发现与运维 | 就绪/健康、etcd Publisher、Prometheus、OTel、SDK/CLI | 真实 etcd 就绪切换/撤销；独立进程 trace 关联、取消和 drain |

## 复现与修正

以下失败来自本轮增量实现与验证，原始日志保留在 `checks.tar.gz` 的 `logs/`，不归因于上游框架：

| 复现 | 根因 | 修正与回归 |
| --- | --- | --- |
| `TestMetadataEncodedLimitIncludesSeparators`：4097 B 的 JSON map 被接受 | 元数据长度统计未计入 key/value 项之间的逗号 | 按实际 JSON 分隔符计数；4096 B 边界与转义仍受限 |
| `TestPipelineRetriesConsumeRateBudget`：burst=1 实际尝试两次 | 只对逻辑调用扣一次令牌 | 每次底层尝试前检查额度；增加真实 RPC 重试与额度验证 |
| `TestPipelineStreamPanicReleasesAdmission`：故障注入后 Active 仍为 1 | 底层 OpenStream panic 时缺少统一收尾 | 补充 panic 安全出口；隔离副本撤销修复可复现失败，当前实现释放名额 |
| `TestServerInterceptorObservesHandlerPanic`：客户端收到 Internal，服务端 hook 却报告成功 | panic 恢复位于 hook 结束回调之后，回调先看到了 nil 错误 | 在 hook 收尾处先恢复并写入 Internal，再执行结束回调；实际 RPC 用例及 race 回归通过 |
| `go vet` 报 Context cancel 错误出口未使用 | keepalive 配置错误在 Context 创建后返回 | 先配置 socket 再创建 Context；最终 vet 通过，此记录不作为实测 goroutine 泄漏结论 |
| 初次版本化消费入口无法导入 `internal/testcert` | 验收脚本把仓库命令源码复制到外部模块，违反 Go internal 可见性 | 外部模块编译公开 observability 包，并通过模块路径运行该版本命令；消费验证通过 |

同时保留初次编译错误与修正后结果；它们是开发记录，未计入性能结果。压缩中途取消仅清理当前帧，不关闭复用连接；管线关闭后的 Wait 仍等待 routing 持有的未读流租约，相关回归包含在全量检查中。

## 最终正确性检查

| 检查 | 结果 |
| --- | --- |
| Go 1.27.3 全量 `go test -count=1 ./...`，真实独占单成员 etcd | 421 pass，0 skip，0 fail |
| 同版本全量 `go test -race -count=1 ./...`，真实独占单成员 etcd | 421 pass，0 skip，0 fail |
| `go vet ./...` | 通过 |
| 最低支持版本 Go 1.26.10 `go build -mod=readonly ./...` | 通过 |
| TCP/TLS/QUIC 独立进程，普通与 race 各一次 | 每次 23 个命令通过；自有 etcd/服务进程全部回收 |
| 三种传输的追踪关联 | 每种传输 5 条客户端完成日志匹配服务端 trace ID，服务端捕获 11 个 span；普通与 race 均通过 |
| Protobuf 再生成与独立 Opaque SDK | protoc 36.2 / runtime v1.36.12；再生成一致，独立模块 race、扩展后重编译通过；双向流与 reserved 字段重用按预期拒绝 |
| 版本化模块消费 | 本地 file proxy、隔离缓存、无 replace；v0.0.0-r15-local 编译/运行 SDK 与完整接入命令，版本化安装插件通过；未发布公共版本 |

独立进程验收使用真实 loopback RPC 与单成员 etcd：返回 64 KiB Protobuf 正文、接收 20 条流响应、取消另一条长流、回传 request-id trailer、校验错误凭证、调用 JSON/Protobuf CLI、查询健康/方法/指标，SIGTERM 后验证 drain 静止、累计交付故障为零且发现前缀已撤销。凭证通过环境变量传递，归档不保存私钥和令牌。

```sh
# 输出目录需要尚不存在；etcd/protoc 放在 PATH 或显式传参。
python3 scripts/verify-discovery.py --toolchain go1.27.3 --output /tmp/r15-normal
python3 scripts/verify-discovery.py --toolchain go1.27.3 --race --output /tmp/r15-race
python3 scripts/verify-production.py --output /tmp/r15-chain
python3 scripts/verify-production.py --race --output /tmp/r15-chain-race
python3 scripts/verify-protobuf.py --protoc /path/to/protoc --output /tmp/r15-sdk
python3 scripts/verify-module.py --version v0.0.0-r15-local --output /tmp/r15-module
```

## 性能与代价

性能结果在冻结源码后串行采集；race、编译及正确性测试不与其并行。默认关闭新增可选功能的路径使用 R14 归档源码作为内部基线，并保留同条件 grpc-go 样本。压缩另用确定性正文微基准观察压缩比例、CPU 与分配成本，不据此推断真实网络吞吐收益。

最终冻结版本：Go 1.27.3，Apple M5 / darwin arm64，GOMAXPROCS=4，TCP/TLS、并发 16、一个复用连接、每组 1 秒、四轮三方交替。新增调用选项、压缩、管线、发现与治理在这组对照中关闭，基线为 R14 最终 `source.tar.gz`；共 72 组、4,089,973 次 offered，失败 0。下表为每轮配对吞吐比的中位数与范围；不是总体容量或显著性结论。完整 p50/p95/p99、CPU、分配、grpc-go 样本见 JSON。

| 传输 | 正文与响应数 | 相对 R14 吞吐变化中位数 | 四轮配对范围 |
| --- | --- | --- | --- |
| TCP | 64 B × 1 | -3.07% | -4.44% … +3.44% |
| TCP | 1024 B × 8 | -9.76% | -18.36% … -2.73% |
| TCP | 4096 B × 1 | -2.57% | -6.18% … -1.64% |
| TLS | 64 B × 1 | +1.48% | +0.39% … +2.80% |
| TLS | 1024 B × 8 | -0.06% | -4.37% … +1.65% |
| TLS | 4096 B × 1 | +0.28% | -2.18% … +1.26% |

TCP 短流最终样本出现 -9.76% 的中位数退化，四轮均为负；处理器 panic 观测修正前的独立四轮扫描，该项为 -3.11%（范围 -3.85% … -2.36%），三项 TCP 为 -1.68% … -3.11%。两组完整样本都保留，不能把差异直接归因于该修正，也不能宣称新增功能关闭后没有额外成本；该项开销尚未进一步归因。TLS 4096 B 的前一组扫描也包含 -18.93% 的下降轮，未剔除。

逐消息 gzip 微基准（两次、100 ms/子项，阈值 1024 B）保留以下代价：

| 输入 | 准备后正文比例 | 每次耗时 | 每次分配 |
| --- | --- | --- | --- |
| 64 B | 1，低于阈值 | 只走阈值分支，不代表 RPC 吞吐 | 0 B / 0 次 |
| 4096 B，重复字符 | 0.01001 | 66.5–68.1 μs | 约 831 KiB / 19 次 |
| 4096 B，固定种子随机字节 | 1，回退原文 | 68.5–68.6 μs | 约 831 KiB / 19 次 |
| 65536 B，重复字符 | 0.001648 | 82.1–85.2 μs | 约 891 KiB / 19 次 |
| 65536 B，固定种子随机字节 | 1，回退原文 | 81.7–83.3 μs | 约 891 KiB / 19 次 |

当前逐消息创建 gzip writer，不持有长期压缩器池。回退原文避免扩大线上正文，但不会撤销已经付出的压缩 CPU/分配成本。重复字符和随机字节是确定性诊断输入，正文比例排除元数据/帧头/加密/重传，不是生产数据压缩率，也没有证明端到端延迟或吞吐收益。

```sh
# 解出 docs/verification/grpc-r14/source.tar.gz 作为内部基线后：
python3 scripts/compare-grpc.py --baseline-root /tmp/r14-baseline --focus \
  --duration 1 --repeats 4 --output /tmp/r15-default
python3 scripts/summarize-grpc.py /tmp/r15-default
GOMAXPROCS=4 go test -run '^$' -bench '^BenchmarkGzipBody$' -benchmem -benchtime=100ms -count=2 .
```


## 证据文件

| 文件 | 内容 |
| --- | --- |
| `normal-result.json`、`race-result.json` | 最终 421 项真实 etcd 检查收据 |
| `chain-result.json`、`chain-race-result.json` | 最终独立进程链路、源码/二进制 SHA-256、依赖与自有 etcd 归属 |
| `sdk-result.json`、`module-result.json` | 再生成、Opaque SDK 和版本化模块消费 |
| `default-result.json`、`default-summary.json`、`default-manifest.json` | 最终 72 组配对明细与汇总 |
| `default-pre-hook-result.json`、`default-pre-hook-summary.json` | 观测修正前的扫描，保留负结果与波动 |
| `checks.tar.gz` | 最终 test JSONL、普通/race 链路原始输出、trace 关联、指标、发现撤销、SDK/版本化消费及初次失败日志；排除执行文件、缓存、etcd data/WAL |
| `default-final.tar.gz`、`default-pre-hook.tar.gz` | 两组对照的所有 offered、服务端、drain、stderr、PID 回收与源码 manifest；不含执行文件 |
| `gzip-bench.txt` | 压缩比例、耗时、分配的全部微基准样本 |
| `source.tar.gz`、`pre-hook-source.tar.gz` | 最终源码与前一组扫描源码；后者还原 panic 观测修正与复现测试两处变更，内部 R14 基线使用既有归档 |
| `verification.json`、`verify-evidence.py` | 源码对应关系、独立逐条请求与关闭核查；不替代真实 RPC 执行 |
| `SHA256SUMS` | 本目录文件校验值 |

```sh
python3 docs/verification/production-r15/verify-evidence.py
```

原有 R13/R14 档案保持不变。

## 使用范围

本轮覆盖普通调用与服务端流；客户端流和双向流保持生成阶段显式拒绝。身份验证/方法授权接入应用策略，QPS 为进程内静态全局/方法令牌桶，endpoint 熔断不能仅凭远端状态码识别全部故障。重试不提供业务去重或副作用回滚，ACK 不代表业务消费或持久化。

标准观测已接入实际 OTel SDK；验收 exporter 保存在进程内，用于验证 span 及关联。远程 collector、外部身份平台、真实双机/WAN、多成员 etcd 分区、小时/天级运行与模型推理性能需要相应环境验证。压缩比例与本机对照不作为真实推理或生产容量收益。
