# R9D：QUIC 小请求退化复验与收尾开销

**结论：原 −6.1% / −3.4% 没有稳定复现。尝试的三个优化候选没有证明整体收益，且出现局部负变化，全部撤回；RPC 库最终保持 R9C 实现。** 本轮交付可复用的诊断工具、最小边界用例与完整证据，不宣称恢复了一个稳定的 6% 回归。

从 R9C `c02d770` 开始，内部参考为 R9B `0b33dad`。[实现前计划](../../quic-r9d.md)。仅处理 KanaRPC，不做 gRPC 对照，CampusTrace 未修改。没有真实双机、物理 WAN/NIC、真实 vLLM 或生产容量验证；所有 payload 为本机 JSON fixture。

## 先验证原来的退化是否稳定

R9C 报告的 128B QUIC managed 普通/流式中位吞吐变化为约 −6.1% / −3.4%，当时每组仅 1000 次、5 次重复。本轮在改动任何运行时代码前，冻结 R9B/R9C 代码、相同 driver、两连接、8 协程，按同一种子配对，交替版本先后；先重复 15 对短跑，再延长到每组 20000 次、15 对。没有并跑本轮测试或 profile。

| 负载 | 每组 offered / 对数 | 每对变化中位数 | 每对中位数 bootstrap 95% 描述区间 | 负变化对数 |
| --- | --- | ---: | --- | ---: |
| 普通 | 1000 / 15 | −0.54% | [−1.72%, +0.42%] | 9/15 |
| 流式 16 chunk | 1000 / 15 | +0.80% | [−1.79%, +6.66%] | 6/15 |
| 普通 | 20000 / 15 | −1.47% | [−2.79%, +0.29%] | 11/15 |
| 流式 16 chunk | 20000 / 15 | +3.09% | [+0.07%, +18.50%] | 4/15 |

[短跑逐对/范围](stability-short-pairs.json)、[延长逐对/范围](stability-long-pairs.json)，共 **120 case / 1,260,000 offered，全部成功**。原 −6.1% / −3.4% 幅度和方向没有稳定复现；普通调用仍有轻微负变化倾向，不能据此断言其成本为零。长流式对照正变化的区间很宽，也不应宣称 R9C 带来收益。共享主机调度/热状态没有硬隔离，bootstrap 只是这批配对重复的描述，不能消除时间相关性或证明因果。

本报告把“每对比值的中位数”作为主要稳定性观察，同时保存原先使用的“两个版本各自中位吞吐之比”，两者不混用。没有删掉负样本、按符号重选种子或用较好的一轮替换较差的一轮。

## 定位到的局部开销与排除项

1. **ACK 后正常 FIN 的重复同步。** R9C 的 `recordStop` 在 QUIC peer FIN 已经取消 Context 后，仍查询 Cause 并取得 Server.mu，才检查已关闭的 ACK channel，随后不修改任何计数而返回。局部隔离基准中，该路径中位约 **50.75ns/op**。真实 100000 次普通/流式诊断的 mutex profile 把约 **2.07ms（总等待 1.27%）/0.848ms（0.40%）** 归因到此调用栈。它确认有不必要的同步，但远不足以解释原先 6% 的整体差异；mutex 归因不是该函数独占 CPU 或可相加的端到端延迟。
2. **没有跨分配级别。** `execution` 从 R9B 的 376B 增至 R9C 的 384B，但受控逃逸分配均为 **384 B/op、1 alloc/op**。不能把结构体增加 8B 写成“每请求多一次分配”或确认的 size-class 回归。两版局部分配耗时本轮也不同，完整数据保留；顺序微基准无法将该变化全部归因于布局。
3. **没有改发送规避策略。** 未回到 quic-go 的已知危险缓冲写法。预算、写入原子批量、回执结算和退休实现均未被削减。

[CPU/mutex/alloc 原始诊断](profiles-before/raw-cases.tar.gz)、[可读栈摘要](profiles-before/summaries/)、[源码/公共 driver 与二进制 hash](profiles-before/metadata.json)、[原收尾微基准](stop-before-bench.txt)、[R9B/R9C 布局基准](layout-commands.json)。profile 使用显式开关，记录与吞吐对照分离；分析工具拒绝将 profiled 运行当作吞吐证据。首次对 R9B 查询 R9C 才有的 recordStop 函数，pprof 以“无匹配”退出 2，该输出保留在 summaries 中，不计为框架故障。

## 三个优化候选与拒绝原因

运行时尝试都限于 drain.go 的 recordStop，保留 BudgetError、池预算、首次取消观察、ACK 结算截止点、迟到 ACK 不翻转故障、累计交付故障与 drain 返回语义。

1. **Err 预检与 ACK 提前返回。** 先检查 Context.Err，已 ACK 且不是请求 deadline 时跳过 Cause 与 Server.mu。局部 acked_fin 从 50.75ns 降至 19.92ns，但 acked_deadline / canceled / transport 分别从 47.96/50.90/64.48ns 升至 57.58/65.29/73.09ns：[候选微基准](stop-candidate-bench.txt)。20 case / 100000 offered 的真实链路全部成功，普通/流式每对中位变化 −0.33% / +1.42%，区间均跨 0：[内部对照](candidate-control-pairs.json)。没有证明整体收益。
2. **复用非 nil Err。** 尝试减少一次已取消 Context 的同步，但后续局部 deadline/canceled 数值仍明显变差：[原始微基准](stop-reuse-candidate-bench.txt)、[候选补丁](rejected-err-reuse.patch)。该轮属于不同进程/时段的局部观察，不能精确归因于某条指令；作为负结果保留，未采纳。[候选 profile](reuse-profiles-after/raw-cases.tar.gz)、[当时源码](reuse-candidate-source-manifest.json)不充当最终运行时证据。
3. **仅为已 ACK 请求提前返回，保留未 ACK 主体。** 故障分类主体与 R9C 逐字节相同，只尝试跳过 ACK 后的统计锁；实际请求 deadline 仍计数，ACK 与取消优先级不变。同一 benchmark binary、同一路径共享 Server，并使用 R9C 原函数精确副本作局部对照。最终 acked_fin / acked_deadline / canceled 的 ns/op 中位数分别为 **49.15→69.02 / 45.51→116.90 / 50.15→53.62**：[配对基准](final-micro-comparison.json)、[原始输出](stop-final-paired-bench.txt)。少拿一把锁没有带来局部成本下降；争用、布局与调度因素未进一步隔离，不凭源码猜测收益。首次配对基准曾与短暂 build/vet 重叠，单独保留为 [诊断输出](stop-paired-initial-diagnostic.txt)，未用于这些最终数字。

第三候选冻结源码后，R9B/R9C/候选做串行小请求内部对照：两连接、8 协程、128B、普通与 16 chunk 流式，5 个种子，每组 10000 次，共 **30 case / 300000 offered，全部成功**，无重复执行或遗留连接/池账户。相对 R9C，普通/流式每对中位变化 **+1.85% / +0.69%**，bootstrap 描述区间分别 **[−0.92%, +3.89%] / [−7.69%, +5.77%]**；相对 R9B 也都跨 0：[对 R9C](conservative-vs-r9c-pairs.json)、[对 R9B](conservative-vs-r9b-pairs.json)、[完整样本](final-small/summary.json)、[原始 offered/日志](final-small/raw-cases.tar.gz)。目录名 final-small 是最后一轮候选实验名，**不表示该候选被采用**。

决定：三种候选均不合入。最终 drain.go 恢复 `c02d770` 原文；[最终源码清单](final-source-manifest.json)逐文件确认全部 RPC 库源码与 R9C 相同。没有删除预算、回执或统计，也没有为了“交付优化”保留无充分收益证据的代码。

## 必要回归与用户要求的范围收缩

在用户要求减少对面试展示无帮助的测试之前，候选已完成 Go 1.27.1 / 最低 Go 1.26.0 的普通和 race 四轮，各 **339 PASS / 0 FAIL / 0 SKIP**：[普通](go-local/result.json)、[race](go-race/result.json)、[最低版本普通](go-minimum/result.json)、[最低版本 race](go-minimum-race/result.json)。每轮真实独立单成员 etcd，进程均回收；[清理收据](etcd-cleanup.json)。保守候选的 ACK/deadline/取消/预算/drain 边界 race 完成 **530 次 PASS**：[结果](conservative-contracts-race.jsonl)。[SDK 独立模块](sdk/result.json)公开 API 示例也通过。它们是候选的正确性证据，不冒充性能收益。

前期另做的模型 fuzz、网络故障重复和旧候选矩阵均保留在 reuse-* 目录。按用户最新要求，后续没有追加 fuzz、长期压力或 TCP/大包性能扩展矩阵；只完成已启动的资源检查、上述小请求对照及回退后的必要 race。回退后完整 race 结果见 [rollback-race](rollback-race/result.json)。未对未运行的范围声称没有退化。

资源检查使用真实 QUIC 和相同 pool/resource driver：R9C/候选各宽预算 60 秒、饱和 30 秒，每组 6 个退休/未读周期。宽预算通过，饱和组保留 ErrDelivery/DEGRADED；最终池账户和字节归零、峰值不越界、空闲 FD/goroutine 稳定。[先前候选与 R9C](resources/summary.json)、[保守候选](final-resources/summary.json)含实际 offered、拒绝、取消与 LostTerminals。保守候选饱和保留 26710 个未回执，未通过清空故障使 drain 变绿。资源观察与部分正确性检查共享主机，不用 offered 数推算吞吐；最终库代码回退后不把这些候选资源结果重新标注成新运行时。

## 可直接用于面试展示的内容

可以准确表述：发现小请求短跑结果下降后，先用配对重复和延长负载检验稳定性；profile 找到 ACK 后正常 FIN 的额外统计锁，但其归因比例不足以解释原降幅；三个提前返回候选都保持协议边界，测量未证明全链路收益且出现局部负变化，因此没有合入。保留累计未回执，资源退出与业务交付成功分别报告。

在仓库根目录可用较短命令重放 R9B/R9C 的真实 loopback 对照（输出目录须不存在）：

```sh
python3 scripts/compare-quic-write.py --versions r9b r9c --payloads 128 --transports quic --strategies managed --seeds 7 19 41 --count 10000 --output /tmp/kanarpc-quic-replay
python3 scripts/analyze-paired.py --input /tmp/kanarpc-quic-replay --baseline r9b --candidate r9c --output /tmp/kanarpc-quic-pairs.json
```

这条演示展示真实请求、延迟、完整 offered、worker/drain 与池清理结果；不用 fixture 冒充真实模型推理，也不需要展示无关测试数量。

## 仍然存在的边界

- 原退化幅度没有稳定复现，不能包装成“修复稳定的 6.1%/3.4% 回归”；局部同步成本也不能乘成端到端收益。每轮完整 offered 和所有负变化均保留。
- 预算饱和可能中断通道，未回执继续造成 ErrDelivery；资源归零不等于业务全部成功，不能隐式重试或抹去累计故障。
- 尚无真实双机、真实 WAN/NIC/MTU、小时/天级运行、etcd 多成员或真实模型推理证据；本地 loopback、UDP relay、有限分钟级资源观察和 fixture 都不代表生产吞吐或推理收益。

下一步优先顺序：真实双机的长时混合负载与滚动 drain；依据受控 profile 优化更显著的 packetization/复制成本；先定义应用幂等与恢复契约，再讨论失败重试。本轮没有为提高数字而扩展这些功能。
