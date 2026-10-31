# R10：可解释的 RPC 治理演示台

从 `71ffc13` 开始，仅修改 KanaRPC。CampusTrace 不动；真实双机、WAN/NIC、真实模型推理均未验证。本轮使用同进程、真实 loopback QUIC socket 的 timer/slot 负载，不宣称生产容量或推理收益。

## 结合代码的实施步骤

1. 复用 `Governance/FeedbackStats/DrainWithReport`，给 ServerConfig 增加默认关闭的本地事件记录器。事件仅含连接/请求标识、方法、阶段、原因及状态码，不改 wire、不记录请求体。固定容量、可采样、TryLock 失败直接丢弃并计数；不在 RPC 锁内调用外部 callback。记录接纳/排队/执行/DATA/取消/END/ACK/结算/退休及治理调整。handler 通过 Context 取得本地请求标识，关联演示流。
2. 新增 `go run ./cmd/showcase`。启动本地 QUIC server、SDK client、限量负载发生器与 HTTP 页面；控制 RPS、慢请求比例、处理延迟和后端槽位。默认有界，不为每个排队请求无限创建 goroutine。切换固定/自适应模式创建明确的新会话，保留旧会话报告和累计交付故障。
3. 三个现场流程：调负载触发过载；真实 SDK Stream/Recv 与用户取消；正常 drain 与专用实验坏客户端故意不 ACK 的 drain。以实际事件和计数展示，不用客户端收到 END 冒充服务端已收 ACK，不清除历史故障。drain 超时或新请求未接纳均展示实际失败。
4. 嵌入离线页面：状态/曲线、操作控制、流式块、请求列表和所选请求时间线。增量取事件，浏览器保留数量也有上限；错误时保留最后一次可用数据，禁止陈旧响应覆盖新状态。
5. 必要验证：记录器有界/默认关闭及关键生命周期的少量针对性检查；新程序 race；真实 HTTP+QUIC 跑通三个操作，浏览器检查和一键进程退出。不开大规模 benchmark/fuzz/长时矩阵，所有失败和退化保留。

## 页面设计计划与自审

界面定位为可操作的 RPC 实验仪表，不做普通指标卡片墙。记忆点放在请求生命周期和原因时间线。

- 色彩：雾蓝白 #F5F7FA、墨蓝 #192D42、钴蓝 #315BD6、青色 #137E86、琥珀 #AC5929、分隔灰 #D3DDE8。
- 字体：本机 PingFang SC / Noto Sans CJK SC / sans-serif；请求 ID 用本机等宽字体。无外部字体、CDN 或 npm 依赖。
- 布局：左侧负载/场景控制；右侧先展示请求经过的生命周期，再放方法状态与曲线；下方请求列表和详情时间线。对齐以左对齐为主，数字右对齐；小屏改为纵向布局。
- 自审：不使用通用大数字 hero、渐变、重复阴影卡片或装饰性编号。图形表达在途阶段和指标变化，颜色对应状态；操作引发的变化可见，失败不隐藏。键盘 focus、减少动画和离线可运行作为基本要求。

验证和实际限制见 [交付报告](verification/showcase-r10/report.md)。

## 启动与现场操作

```sh
go run ./cmd/showcase
```

打开 `http://127.0.0.1:8080`。端口被占用时改用 `go run ./cmd/showcase -listen 127.0.0.1:8090`。Ctrl+C 关闭本次演示；不需要 GPU、etcd、容器或 npm。首次构建仍需已有 Go 依赖缓存或正常模块下载条件。

推荐五分钟顺序：

1. 点“制造过载”，观察慢方法并发调整、客户端反馈限流、排队和快慢请求 p95。可拖动四个旋钮，再点“应用负载”。这里展示真实响应，不预设自适应一定优于固定配置。
2. 点“停止新负载”，然后“开始流式请求”；片段到达后点“取消流式请求”。对照客户端取消状态与时间线中的 CANCEL / handler 返回 / 退休。缺少事件不证明事件未发生。
3. 点“正常 drain”，观察已接纳的实验请求完成及资源退出。再“新建会话”，点“故意不 ACK 后 drain”：资源可以退出，同时保留未回执错误。
4. 再新建会话，顶部历史交付故障继续保留。旧请求标注原会话，避免把旧时间线当作本次状态。

切换固定/自适应模式通过“新建会话”完成，明确结束旧负载。历史摘要最多 8 个，累计交付故障不会随摘要淘汰而减少；本地事件和历史仅保留在当前进程内，不是持久化审计。

## 接入本地决策事件

```go
journal, err := kanarpc.NewTraceRecorder(kanarpc.TraceConfig{
    Capacity: 4096, SampleEvery: 1,
})
// Check err. Enable explicitly; nil remains the default.
server, err := kanarpc.NewServer(kanarpc.ServerConfig{Trace: journal})
snapshot := journal.Snapshot(lastSequence, 512)
```

`RequestTrace(ctx)` 在开启记录时返回本地 channel/request/method 标识。TCP 的 channel 为记录连接，QUIC 的 channel 为原生请求流；不改协议、不传播用户身份，不等同于分布式 TraceID。事件不保存请求体、响应体、错误秘密或回执 token。

客户端本地接纳失败没有服务端请求标识：页面用反馈统计和负载错误展示，不伪造远端事件。本地日志所占容量独立于收发记录预算，不是整个进程 RSS 的硬上限。

- 固定容量 16–16384，读取上限 1024；同一个请求按 channel/request 对稳定采样。
- RPC 写事件使用 TryLock；忙则跳过并累计 Dropped，无用户 callback，无观察器 goroutine。Overwritten 与 SampledOut 分别可见，观察记录丢失不改变协议账本。
- 时间线按本地日志捕获序号展示。跨 goroutine 捕获可能与网络因果顺序不同；本地 DATA/END 写成功不证明远端已消费。ACK 也不证明业务提交。
- 前端最多 240 个请求、每请求 96 个事件、90 个图点；每条流显示最近 32 个块。计数和延迟没有伪造为成功数据；延迟窗口包含失败。

## 管理与使用边界

HTTP 只绑定 loopback，拒绝非本机 Host 和跨 Origin 的控制请求；控制体有大小限制。会话重启/drain 用互斥操作门，并发重复操作返回冲突；操作结束后允许再次新建会话。没有提供生产鉴权、持久化配置或自动重放。

演示会话单独将 QUIC idle timeout 配置为 30 分钟，避免讲解期间默认 30 秒空闲造成 SDK 连接关闭。框架默认值未改。时间更久或连接失败时，新建会话；失败仍显示，不自动重放。
