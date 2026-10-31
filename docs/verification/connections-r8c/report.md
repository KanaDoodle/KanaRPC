# R8C：发现驱动的有界连接生命周期

2026-10-31，darwin/arm64。只修改新版 KanaRPC；CampusTrace 和旧 KanaRPC-Go 未修改。实施步骤、配置、正式接入与所有权见 [connections-r8c.md](../../connections-r8c.md)。R8B 的编码前资源接纳和资源观察另见 [报告](../resources-r8b/report.md)。

## 实现与生命周期

新增独立 `routing` 包，依赖根 RPC 与 discovery，避免根包和 discovery 的导入循环。实际 `*discovery.Resolver` 直接实现 Source，创建者保留其关闭权限；管理器不修改注册或接管 lease。

每次请求先取得管理调用名额和总调用预算，再读取完整快照、校验 freshness/revision/cluster/transport/端点上限，按 endpoint ID 轮转候选；同 endpoint 的并发请求共享建连，等待和交给根 Caller 之前再次检查快照。最多向根客户端交付一次请求，没有后台业务重试、隐式降级或故障后重放。

连接条目覆盖 dialing → active → retiring → closing → reclaimed，全部阶段计入 MaxConnections；管理调用名额还覆盖容量等待、普通调用、未归还的 stream 与进行中的用户解码。失效快照停止新路由并停放连接；明确删除或地址变化退休旧实例，删除再添加同 ID/address 也不会复活旧条目。共享建连由池 Context 和 DialTimeout 控制，单个等待者取消不杀其他等待者，全部离开才取消；迟到成功建连只能关闭，不能重新安装。

根客户端 Pending=0 不等于结果已被消费。`routing.Stream` 持有连接引用，直到 EOF/error/Close 且进行中的 Recv 结束。公开返回类型与根 Caller 不同，未宣称无缝实现旧接口。Client.Close 封闭接纳、取消等待和 IO；Wait(ctx) 等待轮询、建连、关闭和应用引用，阻塞的用户函数或遗忘流不会凭空消失。

## 实际复现与修复

| 已复现问题 | 根因 | 修改 / 证据 |
| --- | --- | --- |
| 初稿 Stream 的用户 MarshalJSON panic 后 ActiveCalls=1，Close/Wait 无法收敛 | 错误分支手工释放，但 panic 绕过该分支 | 统一 defer 归还未转移名额/引用，成功后才移交 Stream；[before](before.jsonl)、[race 重复](panic-config-after.jsonl) |
| 构造后修改 FeedbackConfig，后续建连读到 999 而非原配置 7 | 延迟建连保留调用者的可变配置指针 | New 时复制反馈配置；TLS 配置与 CA 池也按其契约复制；同上回归 |
| MaxEndpoints=1，超量快照从 2 到 10,000 节点时管理侧分配从 3 增到 161 次 | 已判容量拒绝，仍继续建立全部端点索引 | 总量/版本/cluster 前置拒绝后不进入索引循环；[失败](oversized-before.jsonl)、[race 重复](unit-repeat.jsonl) |
| QUIC churn 试跑在失败建连清理中 panic | Dial 返回 error 的 Caller 接口可能包含 typed nil；初稿仍尝试 Close | error 返回值不代表连接所有权，不安装也不关闭它；[原试跑](pilot.txt)、[确定复现](typed-nil-before.jsonl)、[30 轮 race](typed-nil-after.jsonl) |
| 旧反馈测试全量回归卡住 120 秒 | 夹具持有私有 c.mu 阻止退休，又等待可能持有信用锁的读取完成，形成测试自身等待环 | 先断言逻辑 pending.done 与反馈名额，再解锁读取；未据此修改运行时锁/协议；[失败矩阵和堆栈](check-test/test.jsonl)、[100 轮 race](feedback-fixture-after.jsonl) |

R8B 父 QUIC 关闭取消准备后，已有容量拒绝用例还复现了 ResourceExhausted 被 context.Canceled 覆盖。修复归于根客户端：保留父连接的结构化终因，用户取消、显式 Close 与包装的编码错误保持各自身份。前后日志和重复验证见 R8B 报告；没有把所有网络错误都改写成“未执行”。

**手动 Close 的基线**：真实 TCP 流已经完成但 3 块结果未读取时，直接 Close 根客户端会按原契约丢弃缓冲；不能把此操作当成安全的连接退休。新增测试实际复现该行为，再验证管理器在 TCP/TLS/QUIC 上切换地址后保留旧流 10/11/12、新调用进入新节点得到 21；旧流 EOF 后旧连接和缓冲才回收。这不是根 Close 契约的漏洞，而是连接管理必须补上的所有权边界。

## 测试与端到端验证

| 验证 | 结果 / 证据 |
| --- | --- |
| Go 1.27.1 普通 / race，真实隔离 etcd | 各 **213 pass / 0 skip / 0 fail**：[普通](release-test/result.json)、[race](release-race/result.json) |
| Go 1.26.0 普通 / race，真实隔离 etcd | 各 **213 pass / 0 skip / 0 fail**：[普通](release-min-test/result.json)、[race](release-min-race/result.json) |
| 并发建连、取消、容量、快照版本、ABA、迟到成功 | race 重复，32 个首次调用只建 1 条连接；单个等待者取消不杀其他 31 个；所有用户离开后的迟到连接关闭 |
| 退休、关闭、未读结果和用户 codec | 三传输真实 loopback；阻塞解码和忘记 Close 的流使 Wait deadline 如实到期，释放后归零；marshal/unmarshal panic 不漏管理引用 |
| 真实 etcd + TCP/TLS/QUIC | 注册、同 ID 更新地址、删除；旧流保护、新请求选址、删除后拒绝；关闭管理器后创建者拥有的 resolver 仍存在 |
| 真实业务执行后切换地址并关闭旧 server | 三传输原节点恰执行 1 次，新节点未接到重放；新的下一次业务调用才进入新节点 |
| 独立消费模块 | 公共 API 示例在另建 go.mod、GOWORK=off 下编译并实际 TCP 普通/流式调用，race 3 轮：[SDK](final-sdk/result.json) |
| FuzzSnapshotFence，15s / 2 workers | 277,943 executions，PASS：[日志](fuzz-snapshot.jsonl) |
| go vet | PASS：[记录](final-vet.txt) |
| 修复后小规模三传输对照 | 24 case、2,400 offered 全成功：[试跑](pilot-after.txt)；试跑计时不作最终性能证据 |

213 包含子用例、示例与 fuzz seed。etcd 3.7.1 是脚本拥有、核验唯一 member/name/URL 后启动验证并回收的单成员实例，不是生产集群。更早矩阵、失败日志与 [试跑原始记录](pilot-raw.tar.gz)保留，没有删除失败轮次。有限 race/fuzz 和本机进程验证不构成不存在其他竞态/泄漏的证明。

## 最终串行对照

最终对照在资源观察、Go 验证和故障复现全部结束后运行。两个独立真实 worker 与一个独立 client，GOMAXPROCS=2、8 个闭环调用者，每 case 64 次 warmup 后再 offered 1,000 次，种子 7/19/41；TCP/QUIC × 普通/16 块流 × 四种策略。TLS 功能已在真实 etcd 和小规模对照中覆盖。

四种策略使用相同业务与两个 endpoint：direct 直接复用两条连接；dial 每次请求新建连接；managed 配两条连接；churn 两节点仅配一条连接。最终四种策略调用预算均 5s、建连预算均 3s；managed 的实验配置显式覆盖默认 1s。性能 Source 是固定本地 Snapshot fixture，**没有测量真实 etcd 传播时延或模型推理性能**。

各成功/失败 offered 都保留。一次初始串行对照在 QUIC/stream/churn 中完成 992/1,000，另外 8 次返回 deadline（一次共享建连失败），资源归零；原运行保留，不能只取其成功请求 p95 作为整体体验。验证器随后将已输出完整结果、无损坏/重复/资源残留的失败 offered 标记为 DEGRADED 并继续矩阵；崩溃、缺少结果、数据损坏与未收敛仍使验证失败。这是保留负结果的统计方式变更，没有修改运行时去掩盖握手失败。

最终矩阵 **48 case 中 47 个完成测量，1 个在预热建连时失败**。实际测量 offered 为 47,000 且全部完成；另 1,000 个计划测量请求未启动，不能写成 48,000 全成功。失败来自 QUIC/unary/逐次 Dial 的 seed 41，日志为 `warmup: context deadline exceeded`。使用同一已冻结二进制/源码，只续跑尚未开始的 8 个 case，没有重试该失败 case：[续跑校验](continuation.json)、[控制脚本](continue-serial.py)。这项握手问题也出现在直接逐次 Dial 的基线，尚不能据此确定是环境、quic-go 还是 RPC 生命周期原因。不能宣称已修复。

[最终摘要](serialized-summary-complete.json)、[原始 offered/全部日志](serialized-raw.tar.gz)、[源码/二进制 hash](serialized-metadata.json)保留；第一次含 8 个失败 offered 的轮次另在 [原始记录](performance-raw.tar.gz)。源码与 [R8B 最终源码归档](../resources-r8b/final-performance-sources.tar.gz)一致；驱动从异常即中止改为记录并继续的差异单独记录在 continuation receipt 中，未改变 RPC/负载二进制。

每格为完成测量 case 的中位数，含不同种子的短重复；QUIC/unary/dial 只有 2/3，不能忽略其第三次预热失败。p95 只统计成功 offered；CPU 为三个进程完整生命周期，含证书生成、64 次 warmup、启动与关闭。Dials 含 warmup。没有置信区间或最优参数搜索。

| 负载 | 策略 | 完成测量轮次 | req/s | p95 ms | triple CPU s | Dials |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| tcp/unary | direct | 3/3 | 50,403 | 0.251 | 0.1163 | 2 |
| tcp/unary | dial | 3/3 | 20,302 | 0.493 | 0.1976 | 1064 |
| tcp/unary | managed | 3/3 | 46,328 | 0.277 | 0.1174 | 2 |
| tcp/unary | churn | 3/3 | 20,759 | 0.784 | 0.1363 | 141–144 |
| tcp/stream | direct | 3/3 | 11,118 | 1.145 | 0.4647 | 2 |
| tcp/stream | dial | 3/3 | 6,031 | 1.372 | 0.5874 | 1064 |
| tcp/stream | managed | 3/3 | 10,684 | 1.162 | 0.4693 | 2 |
| tcp/stream | churn | 3/3 | 5,664 | 2.839 | 0.5088 | 136–137 |
| quic/unary | direct | 3/3 | 21,407 | 0.549 | 0.2394 | 2 |
| quic/unary | dial | 2/3 | 3,017 | 3.743 | 1.3627 | 1064 |
| quic/unary | managed | 3/3 | 20,681 | 0.601 | 0.2401 | 2 |
| quic/unary | churn | 3/3 | 7,676 | 2.268 | 0.3726 | 134–136 |
| quic/stream | direct | 3/3 | 10,603 | 1.120 | 0.4648 | 2 |
| quic/stream | dial | 3/3 | 2,445 | 4.592 | 1.7157 | 1064 |
| quic/stream | managed | 3/3 | 10,605 | 1.131 | 0.4602 | 2 |
| quic/stream | churn | 3/3 | 4,314 | 3.967 | 0.6571 | 133–134 |

相对直接复用的强基线，managed 的普通请求吞吐在 TCP/QUIC 上分别降低约 **8.1%/3.4%**；TCP 流式降低约 **3.9%**，QUIC 流式基本持平。它增加发现检查、共享建连和引用管理等工作；本实验没有用 profile 将差异拆分归因，不宣称比直接复用更快。

两节点只配一条连接的 churn 策略每轮约 133–144 次建连，吞吐比直接复用低约 **49%–64%**；TCP 流式甚至低于逐次 Dial 的这组结果。managed 在这个两节点固定视图下维持 2 条连接，避免了逐次 Dial 的 1,064 次建连，但这是连接复用与生命周期管理的功能收益，不能包装成胜过所有连接池、真实 etcd 时延或推理收益。

完成测量的 case 均检查数据完整、无同节点重复执行、两个 worker 的计数与 offered 完成范围一致，关闭后池连接/管理名额和 server 终态资源归零。预热失败 case 的关闭日志与 worker 结果也保留；没有将“无观察到重复”写成 exactly-once。

## 风险与生产下一步

尚未解决的风险：本机重复新建 QUIC 时偶发握手/建连超时的根因尚未确认；发现最后检查与实际写入之间仍可发生更新；Source 违约阻塞、用户 codec 阻塞和忘记归还流只能被有界统计/Wait 暴露，不能强杀。候选轮转不提供等待者 FIFO 公平。固定退避没有抖动；没有业务重放、exactly-once、方法 ACL 或租户认证。根字节预算按连接配置，池整体潜在占用会乘以 MaxConnections。测试没有覆盖生产多成员 etcd、真实网络变化或真实 vLLM/GPU。

若面向生产，下一步最值得投入的三项：

1. **R8D 网络与长时故障验收**：Linux 双机、RTT/丢包/限带宽、持续连接 churn、小时/天级负载、etcd 多成员分区；采集 qlog/抓包定位已观察到的偶发 QUIC 建连超时，保留失败并重验取消/终态/drain。
2. **降低控制面进入请求热路径的成本**：事件驱动的不可变快照视图与合并更新，保留最终 freshness 门槛；为连接数小于节点数提供明确的缓存/子集策略，和 direct reuse 继续对照，避免把逐请求 Dial 当唯一弱基线。
3. **增加池级共享字节预算与资源归属统计**：把各连接的编码/接收/发送容量组合成可配置总量，覆盖退休和未读流；用慢消费者、恶意编码、端点 churn 验证组合上限，继续区分有效框架记录预算与用户对象/RSS。
