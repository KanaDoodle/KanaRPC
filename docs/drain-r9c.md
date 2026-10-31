# 过载、取消与优雅关闭契约（R9C）

在 R9B `0b33dad` 的基础上，为本机预算拒绝提供类型化错误，并用累计生命周期观察与 `DrainWithReport` 区分资源退出、取消和交付故障。

## 实现与验证路径

1. 保存最小反例。`byteBudget` 将框架配额拒绝编码成普通 `Status`，无法与业务主动返回的 ResourceExhausted 区分；`Canceled` 混合状态码与真实取消，handler 返回之后收到的 CANCEL/截止时间又没有对应计数；Drain 在截止时间退出时只返回 context 错误，历史交付故障容易被遗漏。
2. 为本机字节预算错误提供类型和预算类别/拒绝层级，保留 Status 兼容性。保持编码、接收、发送、控制预算和池级原子接纳/退休规则。本机框架字节拒绝以 BudgetError 区分业务 ResourceExhausted；远端 reset 保留为通道故障观察。
3. 增加有界累计生命周期计数：接纳拒绝原因、每个有效请求最多一次的 CANCEL 观察、实际请求截止时间、连接中断/关闭，以及 ACK/取消/截止时间/未回执的终态结算。保留现有 Canceled 和 LostTerminals 语义，ACK 在结算前已被观察时优先；终态只结算一次。
4. 增加 DrainWithReport，保留 Drain 入口。封闭接纳时采集 Before；退出时采集 After、内存和资源静止状态，报告历史/本次新增/累计交付故障。既有故障继续使 drain 返回 ErrDelivery；同时超时则保留两个错误。累计故障持续保留，ACK 按协议回执结算。
5. 按路径回归：普通/流式、TCP/TLS/QUIC、重复取消、ACK 竞态、无 ACK、原生 FIN 缺失、排队取消、预算不足、忽略取消的 handler。运行普通/race 完整测试（独立真实单成员 etcd）、SDK 验证、必要的局部 benchmark 和资源故障基线。保留失败与退化，环境口径见 [使用约束](operating-notes.md)。

## 计数边界

预算拒绝、主动取消和终态未回执是不同观察轴，可以重叠。例如服务端发送预算拒绝可产生 ResourceExhausted END 并获得 ACK；客户端接收预算拒绝可能仅让服务端看到 reset 和未回执。饱和导致的未回执保留为交付故障。预算 Rejected 统计失败的预约次数，同一 RPC 可以触发多个观察项。累计交付故障在后续成功调用或重复 drain 后仍保留。

## 正式调用方式与解释

```go
var budget *kanarpc.BudgetError
if errors.As(callErr, &budget) {
    // 本机框架拒绝：Class 和 Scope，以及 Requested/Used/Limit。
    // errors.As(callErr, &status) 仍可取得 ResourceExhausted。
}
shutdown, stop := context.WithTimeout(context.Background(), 3*time.Second)
defer stop()
report, err := server.DrainWithReport(shutdown)
// 分别检查，err 可能同时匹配两个错误。
deliveryFailed := errors.Is(err, kanarpc.ErrDelivery)
deadlineReached := errors.Is(err, context.DeadlineExceeded)
// report.Quiescent 与 deliveryFailed 可以同时为 true。
```

可运行示例：[drain_example_test.go](../drain_example_test.go)。`Drain(ctx)` 保留原入口与单纯 ErrDelivery 的错误值；同时发生上下文错误时使用 errors.Join，调用者应使用 errors.Is，不应解析错误字符串。

| 观察 | 语义 |
| --- | --- |
| MemoryStats 各类 Rejected / BudgetError | 编码、接收、发送、独立控制预算的本机预约失败；先触发的 owner / pool_class / pool_data / closed_owner 层级 |
| CapacityRejected / PolicyRejected | OPEN 未接纳的在途/会话容量与治理排队策略拒绝；没有 ACK 义务 |
| DrainingRejected / InvalidRejected / NotFoundRejected | 因停止接纳、输入/方法形态错误、方法不存在而拒绝；与前两项之和等于 Rejected |
| CancelRequests | 活跃执行第一次收到的有效 CANCEL，包括 END 后的 CANCEL 和客户端 deadline 触发的控制；结算结果由回执字段单独记录 |
| RequestDeadlines | 执行收尾时已观察到自身 Context 的 DeadlineExceeded；按请求自身 Context 计数，与通道关闭 cause 区分 |
| BudgetAborts / TransportAborts / ShutdownAborts | 已接纳执行因本机字节拒绝关闭通道、其他通道故障、服务端关闭而终止；不推断对端 reset 动机。已 ACK 的正常 FIN/关闭不计入这些项 |
| ReceiptsResolved | 已结束的带回执终态结算次数；等于 Delivered + LostTerminals + ReceiptsCanceled + ReceiptsDeadlineExceeded，活跃等待不在此总数内 |
| ReceiptsCanceled / ReceiptsDeadlineExceeded | 未得到 ACK，但显式 CANCEL 胜出 / 自身请求 deadline 到期；与仅仅 handler 返回同名业务 Status 分开 |
| LostTerminals / IncompleteExchanges | ACK 未确认 / ACK 已确认但 QUIC 客户端 FIN 未确认的累计故障；后续成功调用和 drain 不清零 |

未接纳的控制 END 可能发送失败，或 handler 尚未产生终态就被取消，此时不制造一个“已发送终态未回执”计数。各观察项可以重叠。报告为返回时快照，尚未退出的处理器仍可增加累计计数。MemoryBefore 与 RPC Before 分别在各自锁下采集。

结果和实际执行步骤见 [验证报告](verification/drain-r9c/report.md)。
