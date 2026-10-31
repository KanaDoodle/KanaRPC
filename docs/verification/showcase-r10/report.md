# R10：可交互 RPC 决策演示台

已完成一键启动、四个负载旋钮、请求决策时间线，以及过载、流式取消和 drain 三个现场流程。[实施前步骤与启动说明](../../showcase-r10.md)。CampusTrace 没有修改。

```sh
go run ./cmd/showcase
```

访问 `http://127.0.0.1:8080`。有端口冲突时使用 `-listen 127.0.0.1:8090`；Ctrl+C 退出。页面与脚本全部嵌入 Go 程序，无 npm/CDN、GPU 或 etcd 依赖。

## 实际交付

- **真实链路**：同一个进程内的 HTTP 控制、SDK 客户端、QUIC listener 和 handler，通过真实 loopback UDP/QUIC 通信。慢服务由 timer/slot fixture 控制，不是模型推理或生产服务。
- **可调负载**：0–200 RPS、慢请求比例、5–300ms 处理时间、1–16 个后端槽位；最多 40 个发生器调用在途，容量丢弃单列，避免无限创建 goroutine。固定/自适应模式通过明确的新会话切换，旧会话失败与累计交付故障保留。
- **本地时间线**：可选 ServerConfig.Trace，默认关闭。按 channel/request 关联接纳、调度、实际 handler、DATA、取消、END、ACK、结算与退休；handler 可从 Context 取本地标识。没有新增 wire 字段或用户 callback，不记录 payload/回执 token。
- **有界观察**：固定环形日志、稳定请求采样、TryLock 跳过忙日志；覆盖、争用丢弃和采样跳过在页面可见。RPC 不等待浏览器或观察器。默认关闭仍有分支成本，开启也有时间/锁/分配成本，本轮不宣称无开销。
- **可操作页面**：快慢延迟、治理调整原因、反馈限流、负载结果、流式块、会话标识和可点选时间线。客户端 p95 包含失败；控制器 handler p90 独立标注。请求/图点/块显示均有数量上限；断连保留最后数据，旧响应不覆盖新操作状态。

## 三个流程的实际验证

[最终真实 HTTP/QUIC walkthrough](smoke-final/result.json)使用 race 构建的程序，拥有自己的动态本地端口和 PID，结束后 SIGTERM 并回收，**PASS，退出 0，无 race 报告**。

| 流程 | 实际观察 | 状态摘要 |
| --- | --- | --- |
| 过载 | 接纳至少 30 次，慢方法出现 12→9 的真实降低并发决定；该次客户端反馈限流 36 次。展示实况，不判断自适应优于固定配置 | [过载](smoke-final/overload.json) |
| 流式取消 | 收到至少 3 个真实块再取消；客户端进入 canceled，服务端在途和客户端接收字节归零，CANCEL 累计增加 | [流式](smoke-final/streaming.json)、[取消](smoke-final/canceled.json) |
| 正常 drain | 启动一个实际在途 handler，再停止接纳；返回无错误，Quiescent=true，LostTerminals=0 | [正常关闭](smoke-final/normal-drain.json) |
| 故意缺 ACK | 独立实验坏客户端读取真实 END 后不回 ACK；SDK 不增设绕过开关。Quiescent=true，同时返回 ErrDelivery、LostTerminals=1 | [缺回执](smoke-final/missing-ack-drain.json) |
| 故障保留 | 新建固定模式会话，本会话 LostTerminals=0，但进程历史总数仍为 1，并保留旧报告 | [新会话](smoke-final/history-retained.json) |

浏览器另实际点击了过载、流式开始/取消、正常 drain 和缺 ACK drain；看到慢方法 12→9→7 的调整和完整取消观察。手动旋钮设置 5 RPS / 7 槽位并应用后，真实请求进入接纳。最终故障请求时间线显示 END 提交后约 704ms 未获回执，随后请求释放，页面没有把资源退出显示成全部交付成功。[浏览器记录](browser-verification.json)、[实际状态](browser-final-fault-state.json)、[页面截图](showcase-final.png)。

## 必要验证与保留的失败

- `go test -race -count=1 -timeout=90s . ./cmd/showcase`：[最终结果](race-release.txt)。包含原有 RPC 回归，以及少量新增检查：日志有界/写者不等读者、带观察器的 ACK/取消语义、跨站控制与重复操作边界。
- `go vet ./...`：[最终结果](vet-release.txt)。程序正常构建与 race 构建均通过；真实脚本验证了拥有进程的正常退出。
- 最终补充了关闭记录时不额外分类错误的旁路，再次通过 race，并用发布态页面复验缺 ACK 与历史保留：[发布态实际状态](release-live-state.json)。
- 没有新增 benchmark、fuzz 或长期压力矩阵。没有把其他阶段测试数量或旧性能数字算成本轮收益。

实际失败原样保留：

1. **演示闲置后的连接关闭。** 初次页面讲解超过默认 idle 期限，首次负载观察到 **6727 次失败、零接纳**：[现场状态](idle-failure-before.json)。演示专门将 client/server 的 QUIC idle 配为 30 分钟，并增加最近 RPC 错误、历史失败摘要；框架默认参数与连接策略未改。没有把失败伪装为正常服务。
2. **walkthrough 对可选 JSON 字段的误判。** 初版脚本将 `omitempty` 的空 drain_error 当成必填字段，正常 drain 后触发 KeyError：[失败结果](smoke-initial/result.json)、[当时实际状态](smoke-initial/failure-state.json)。修正脚本按空错误读取后通过，[修正版本](smoke-fixed/result.json)和最终版本分别保留，没有归类为 RPC 协议故障。
3. **预期退化**：故意不 ACK 的场景保持 drain_failed/ErrDelivery；历史故障跨新会话保留。记录器忙时可以丢事件，页面显示丢弃数；不得把缺少事件理解为该事件未发生。

## 限制与展示口径

- 真实双机、WAN/NIC/MTU、真实 GPU/vLLM 和生产容量均未验证。默认真实 QUIC 并不意味着本轮做了 TCP/gRPC 性能对照。
- 这是单进程演示。timer/slot 用于控制场景，不代表推理调度或实际业务收益；本轮没有重新证明自适应或 QUIC 的性能优势。
- 日志按本地捕获序号展示，跨 goroutine 可能出现观察顺序与网络因果不同。固定容量与采样允许不完整时间线；协议统计才是累计账本。ACK 不等于用户消费或业务提交。
- 历史、记录与累计值保留在当前演示进程内，不是持久化审计。退出后重新开始；本轮验证文件单独保存失败。
- 本地管理页不提供生产鉴权。缺 ACK 客户端只在实验入口使用；没有修改 SDK 的正常回执行为或引入业务重放。

面试可直接演示：调低容量制造过载 → 选一条请求解释调度/拒绝 → 流式接收后取消 → drain 区分资源退出与交付失败。重点讲状态机、资源归属和观察边界，不用测试数量或模拟吞吐包装收益。

完整原始状态与决策事件无损归档在 [raw-states.tar.gz](raw-states.tar.gz)，[打包收据](packaging-receipt.json)逐成员校验 SHA-256。相邻 JSON 保留状态、计数和原始事件索引，避免把大量重复事件展开到代码审查差异中；失败记录没有删除。
