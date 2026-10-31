# R8A：RPC 自适应控制的显式回退与人工复位

2026-10-31，darwin/arm64，Apple M5。用户要求 CampusTrace 重构期间先处理 RPC 本身，本轮只修改新版 KanaRPC。CampusTrace、旧 KanaRPC-Go、业务数据库和远端发布均未修改。

实现步骤、配置和契约见 [control-r8a.md](../../control-r8a.md)。当前阶段是 R8A，后续 RPC 资源接纳、发现驱动连接生命周期与真实网络验收分别列为 R8B/C/D；CampusTrace 的 R7D 迁移暂缓。没有把未来步骤计为已完成。

## 复现与根因

R7C 已有短时实验确认：处理器固有耗时约 20ms，却设置 5ms 驻留目标，AIMD 会不断降低到 Min=2。本轮在修改前保存 [R7C 源码](r7c-source.tar.gz)及 [hash](r7c-source-manifest.json)，再次跑独立双进程、相同种子与到达过程的三组基线：每组 440 次 offered，固定高并发/公平 440 OK，原自适应 296 OK，原反馈辅助 275 OK。原始日志与完整 offered 见 [baseline](baseline/summary.md)。基线 Go/module hash 与保存快照一致。

根因是控制契约缺少退化处置：超目标即降并发，没有依据证明降并发还能改善这个目标，也没有显式静态回退。这里的驻留时间还包含依赖/信用等待，无法仅凭延迟样本推导真实服务耗时或安全容量。这是已确认的策略退化，不是凭代码猜测的程序漏洞。

## 修改内容

- **可选配置回退**：方法策略新增 FallbackLimit/FallbackWindows。保护默认关闭；开启时静态值必须由服务端配置并受 Min/Max/global cap 限制，窗口数默认 3，范围 1..64。在 Min 上连续成功有效窗口仍超目标才锁定为 fixed_fallback。
- **失败与取消边界**：执行错误清空连续计数并抑制当期回退；用户取消中性。固定回退之后保持配置值，不会因延迟改善自动恢复自适应，也不承诺该静态值安全或最优。排队超时仍单独处理，不采入执行驻留窗口。
- **显式人工复位**：本进程 ResetAdaptive(method) 恢复 Initial，清空当前窗口，递增 generation 和 resets。没有暴露远端管理入口、后台重试或业务重放；draining/closed 明确拒绝复位。
- **在途样本隔离**：请求取得执行槽时捕获 generation。旧 generation 仍正常响应和回收执行名额，但不污染新窗口。队列、执行、ACK/FIN、取消和 drain 的原有归属不变。
- **状态与审计**：每方法公开 mode、连续计数、generation、fallbacks/resets；决策记录前后 mode/generation，仍为 128 条有界环。人工操作即使不改变数值也记录。压力帧协议保持 v1，使用当前方法限额；客户端在下一次 END 获得建议，旧建议仍按原 TTL 过期。
- **更强静态对照**：负载工具增加 fixed8-fair、guarded、guarded-feedback。保护组配置静态回退 4；没有借此宣称超过最佳静态参数或创造了新的容量控制算法。

单元与真实传输验证包括：原 AIMD 无保护仍降至 Min；连续窗口与固定锁定；可达目标和取消不误推进；Unavailable/ResourceExhausted/deadline/Internal/InvalidArgument 窗口不触发上调；配置校验与复制；200 次复位仅保留 128 条决策；TCP/QUIC 上 160 次并发人工复位与快照、旧在途完成、无重复业务调用；真实回退/复位后的客户端建议限额更新和资源回收。

这些是本轮新增能力及其测试，没有将顺利通过的断言包装成新增漏洞数量。本轮未发现需要另行修改的队列、取消或连接状态机缺陷，未做无关重构。源码、来源与依赖检查见 [origins.json](origins.json)。

## 回归与实测

| 检查 | 结果 | 记录 |
| --- | --- | --- |
| Go 1.27.1 普通 / race，独立真实 etcd | 各 163 pass / 0 skip / 0 fail | [verified-test](verified-test/result.json)、[verified-race](verified-race/result.json) |
| Go 1.26.0 普通 / race，独立真实 etcd | 各 163 pass / 0 skip / 0 fail | [verified-min-test](verified-min-test/result.json)、[verified-min-race](verified-min-race/result.json) |
| 保护、复位及真实反馈选定用例 race 重复 | PASS | [guard-initial.jsonl](guard-initial.jsonl)、[real-pressure-race.jsonl](real-pressure-race.jsonl) |
| 控制状态 fuzz，15s / 2 workers | 5,409 executions，PASS | [fuzz-guard.jsonl](fuzz-guard.jsonl) |
| go vet | PASS | [vet.txt](vet.txt) |
| 双进程 SIGTERM、流式、取消、drain | PASS | [core-regression](core-regression/result.json) |
| 新旧取消/drain 行为对照 | 18 轮通过，非性能结论 | [behavior-regression](behavior-regression/comparison.jsonl) |
| TCP/TLS/QUIC 普通/16 块流回归 | 18 组、18,000 次 offered 全成功 | [transport-regression](transport-regression/summary.json) |
| 实际 worker/client race 二进制 | 3 组、6,040 次 offered，允许预期接纳拒绝，资源检查通过；实际观察到固定回退 | [lab-race](lab-race/summary.json) |

163 包含子用例与 fuzz seed。etcd 为本机独立 3.7.1 单成员，每轮核验唯一 member/name/URL 所有权后才执行 compaction，并回收自己的进程。有限 race/fuzz 不证明不存在所有竞态或泄漏；单成员 etcd 也不等于生产集群验收。

## 最终串行对照与负结论

同一最终 Go 源码/二进制，完成全部测试后串行运行：GOMAXPROCS=2，4 条物理客户端连接，每 case 开放到达 2s，种子 7/19/41，轮转策略顺序。生成器同时最多 128 调用，晚于计划 50ms 的 offer 单独记录；不重试丢弃或拒绝。8 策略 × 4 场景 × 3 种子共 96 TCP case，另有 6 TLS/QUIC 保护+反馈兼容 case，合计 **102 case、162,720 offered**。

业务是实际 loopback RPC 包裹的 128 字节定时器/槽位 fixture，无模型。低负载快/慢到达率为 10/10 每秒；稳态与容量切换为 200/1200，慢耗时约 4ms、槽位分别固定 8 或 8→2→8、目标 10ms；不可达场景为 20/200，慢固有耗时约 20ms、槽位 4、目标 5ms。快请求约 1ms、目标 4ms。方法公平控制的是启动顺序；后端槽位门不保证业务完成 FIFO。

所有策略全局执行上限 16、共享队列 64、队列等待上限 60ms、客户端 deadline 250ms、窗口 16。固定参照分别为慢方法 4/8/16；原自适应和保护均以 16 起步、最低 2，保护静态回退值 4、连续窗口 3。客户端反馈只对 feedback/guarded-feedback 开启，服务端压力元数据所有策略一致。不预热，保留冷启动和降并发/积累窗口的代价。

“按时完成”另以从计划发送时刻起快≤20ms、慢≤50ms 统计，包含生成器和 goroutine 调度迟到，与执行驻留控制目标不同。成功分位数只含成功请求；CPU 为两个进程全部生命周期，包含初始化、admin 采样、负载生成和关闭。每组只有三个短时重复，无置信区间，也没有穷举最佳静态值。

**不可达目标**，每策略累计 1,320 offered：

| 策略 | OK | 按时完成 | 本地拒绝 | deadline 错误 | 最终慢方法状态 |
| --- | ---: | ---: | ---: | ---: | --- |
| fixed-fifo | 1,313 | 937 | 0 | 7 | static / 16 |
| fixed-fair | 1,310 | 947 | 0 | 10 | static / 16 |
| fixed4-fair | 1,314 | 706 | 0 | 6 | static / 4 |
| fixed8-fair | 1,319 | 748 | 0 | 1 | static / 8 |
| adaptive | 885 | 408 | 0 | 435 | adaptive / 2 |
| feedback | 818 | 810 | 502 | 0 | adaptive / 2 |
| guarded | 1,158 | 443 | 0 | 162 | fixed_fallback / 4 |
| guarded-feedback | 887 | 878 | 433 | 0 | fixed_fallback / 4 |

保护模式相对原自适应完成量 **+30.8%**，但仍比固定 8 少 **12.2%**；按时完成仅从 408 到 443，没有恢复到静态基线。带反馈保护相对原反馈完成量 **+8.4%**、按时完成从 810 到 878，但仍低于固定高并发/公平的 947 次按时完成，也比固定 8 少 **32.8%** 的完成量。因此只能说缓解了这个已确认退化，不能宣称解决了不可达目标、自动找到了最佳容量或普遍胜过静态限额。

三轮保护都恰好回退一次，无自动来回切换。20ms 状态采样首次观察到回退的时刻，guarded 约 1.10s，guarded-feedback 约 1.24–1.30s；2s 冷启动 case 已付出了前半段降并发与窗口积累的代价。采样时间是客户端观察时刻，不是控制器精确决策时钟，也不是引擎首 token 时间。

**容量切换**，每策略累计 8,400 offered；快 p95 是三轮成功请求 p95 均值：

| 策略 | OK | 按时完成 | 快 OK / 1,200 | 快 p95 ms | 本地拒绝 | ResourceExhausted | deadline 错误 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| fixed-fifo | 6,966 | 5,793 | 995 | 60.752 | 0 | 591 | 843 |
| fixed-fair | 7,154 | 6,145 | 1,160 | 4.411 | 0 | 318 | 928 |
| fixed4-fair | 6,145 | 1,864 | 1,200 | 1.528 | 0 | 756 | 1,499 |
| fixed8-fair | 7,176 | 6,210 | 1,200 | 1.521 | 0 | 279 | 945 |
| adaptive | 7,131 | 6,067 | 1,200 | 1.529 | 0 | 286 | 983 |
| feedback | 6,844 | 6,830 | 1,200 | 1.567 | 1,556 | 0 | 0 |
| guarded | 7,115 | 5,996 | 1,200 | 1.536 | 0 | 294 | 991 |
| guarded-feedback | 6,857 | 6,847 | 1,200 | 1.565 | 1,543 | 0 | 0 |

保护组没有触发回退，最终仍为 adaptive。相对原自适应，guarded OK **-0.2%**、按时完成 **-1.2%**，进程对 CPU 均值从 1.255s 到 1.420s；原样保留，三轮不足以证明这些差异的统计显著性或唯一根因。固定 8 的完成量与按时完成都高于两组仅服务端自适应。保护+反馈相对固定 8 按时完成更多，但完成量更少，仍是提前拒绝与等待之间的取舍。

低负载每策略 120 次全成功，无吞吐收益证据。稳态除了固定 4 只有 7,091/8,400 成功，其余策略各 8,400 次全成功；保护并没有增加这些场景的完成量，也未证明普遍 CPU/延迟收益。低负载、稳态、容量切换均没有误触发回退，但这只是本轮有限 fixture，不能保证异质真实请求或慢消费者下不误触发。

TLS/QUIC 兼容组分别 8,400 offered、6,821 / 6,845 OK，其余为预期本地接纳拒绝。它们只验证保护+反馈链路，不是公平的跨传输性能比较，也没有覆盖真实跨机丢包/限带宽。

全部 case 合计 **144,909 OK、7,168 本地反馈拒绝、7,391 deadline 错误、3,252 ResourceExhausted = 162,720 offered**；没有生成端迟到/限额丢弃、响应损坏、未分类传输错误或重复业务 ID。每 case 连接、排队、执行、在途、原生流、丢失终态、未完成交换归零；字节 Used=0、Peak≤Limit，客户端反馈 Active=0，自有进程均回收。这是有限资源尾部检查，不能替代长期 goroutine/heap 曲线。

完整汇总见 [summary.md](comparison-final/summary.md)、[summary.json](comparison-final/summary.json)；分组状态与回退观察时间见 [analysis.json](comparison-final/analysis.json)，全部 offered/状态样本/log/公用 CA 在 [cases.tar.gz](comparison-final/cases.tar.gz)。source/module/driver/binary hash 见 [metadata.json](comparison-final/metadata.json)。未保存私钥。当前源码包和最终一致性结果见 [source.tar.gz](source.tar.gz)、[verification.json](verification.json)，证据文件哈希见 [SHA256SUMS.json](SHA256SUMS.json)。真实 vLLM/GPU、跨机网络和长期 heap/goroutine 均未验证。

## 保留的设计风险与下一步

1. **回退不等于可达性检测**：持续成功的真实拥塞、异质请求或慢消费者也会超目标。错误的固定值可能增加排队/驻留延迟；没有自动估算安全容量，默认不替用户启用。窗口按样本而非墙钟划分，初始突发与短流变化仍受采样速度影响。
2. **锁定与人工操作有代价**：固定回退可能浪费恢复后容量；人工复位错误目标仍会再次触发。复位不抢占现有请求，客户端建议也只能等响应/TTL 更新。处理器忽略 Context 的行为仍无法强制中断。
3. **方法公平不是租户隔离**：仍无可信租户成本、每租户队列或分布式配额；多连接建议仍可能超卖，公共队列仍可能被单方法占满。
4. **资源与网络边界未扩大**：本轮没有解决客户端编码早于在途接纳的额外内存，没有全进程内存上限，没有发现驱动连接池或跨机丢包/限带宽结论。没有新增运行依赖，未宣称完整生产可用。

下一步优先 R8B：编码前并发接纳、编码结果预算和长期资源曲线；然后 R8C：发现更新、连接建立去重、旧连接退休与独立 SDK；最后按可用环境跑 R8D 真实网络与多成员 etcd。CampusTrace 迁移继续等待其重构稳定。所有 fixture 的结果均不代表真实推理收益。
