# R7B：安全传输、原生流和独立发现

基于 R7A 的调用契约，重新实现 TLS、QUIC 接入与 etcd 注册发现。没有导入旧 KanaRPC-Go 内核；quic-go 和 etcd client 是明确声明的外部依赖。真实模型、跨机网络和生产流量另行验收。

## 分阶段实施

| 阶段 | 实现 | 验证 |
| --- | --- | --- |
| B1 | recordChannel 与物理连接分开；TLS 1.3、固定 ALPN、信任配置、可选 mTLS、有限握手 | R7A 回归；证书/主机名/ALPN/缺客户端证书；慢握手不阻塞接收 |
| B2 | 一个 QUIC session 多个原生双向流，每流恰好一个请求交换；独立取消/坏帧；FIN 与 session 结束屏障；明确 UDP 所有权 | 三种传输语义、TCP/TLS 故障对照、容量/首帧/ACK/slot 等待、取消、drain、立即关闭和 UDP 重绑 |
| B3 | 独立 discovery 包；显式 v2 namespace、lease + 所有权 CAS、线性一致快照 + revision watch、progress 新鲜度 | 真 etcd 注册、续租丢失、替换所有者、租约撤销、Get/Watch 间隙、同 revision 多键事务、compaction、TCP 断流门、发现后真实调用 |
| B4 | 最低 Go 版本与当前 Go 的完整普通/race；fuzz、独立进程负载、原始结果和来源记录 | 含失败/退化，保留 R7A 源码快照；不将本地收益外推到真实推理 |

## 公共入口

普通处理器、Call/Stream/Recv、Close/Drain 仍遵守 R7A 语义。客户端共享 `Caller` 接口，传输的会话资源并不伪装成同一个 net.Conn。

```go
// config is a caller-owned tls.Config, immutable after construction.
options := kanarpc.TLSOptions{Config: config}
client, err := kanarpc.DialTLS(ctx, address, options, kanarpc.ClientConfig{})
// Or one authenticated session, one native stream per call:
native, err := kanarpc.DialQUIC(ctx, address, options, kanarpc.QUICConfig{}, kanarpc.ClientConfig{})
// Check errors and defer client.Close()/native.Close().

endpoint := kanarpc.Endpoint{Address: address, Transport: kanarpc.QUIC}
caller, err := kanarpc.DialEndpoint(ctx, endpoint, kanarpc.DialOptions{TLS: options})
```

服务端 `ServeTLS(net.Listener, TLSOptions)` 在接受 socket 后执行有期限的握手，握手中连接也占 MaxConnections。`ListenQUIC` 显式创建 packet socket/transport/listener，`ServeQUIC` 成功后将其所有权交给 Server。Serve 返回只表示接纳循环已结束，仍使用 Drain/Wait 等待请求和资源；不要在 Serve goroutine 返回时自行关闭已转交的 QUIC transport。

TLS/QUIC 固定 ALPN 分别为 `kanarpc-v2-tls` / `kanarpc-v2-quic`。拒绝 InsecureSkipVerify、TLS 1.2 上限和 GetConfigForClient 动态替换；允许 GetCertificate 选证书。mTLS 只支持 RequireAndVerifyClientCert 并要求 ClientCAs。mTLS 认证不等于方法或租户权限。

QUIC 默认最多 64 个接收双向流；禁止接收单向流，不使用 early dial、0-RTT 或 datagrams。接收窗口起始每流 64 KiB/连接 512 KiB，最大每流 1 MiB/连接 8 MiB。首记录默认 3 秒，空闲 30 秒。服务端总执行与每物理会话名额仍由 R7A 的 MaxInFlight / MaxPerConnection 管理，原生流数量不冒充物理连接或执行量。QUIC 的 MaxConnections 在握手被接收后计数，不是完整抗 DoS 边界。

## QUIC 交换结束与会话关闭

对正常已接纳调用：END receipt → 客户端 ACK → 服务端 FIN → 客户端观察 EOF 后发 FIN。服务端在观察客户端 FIN 后才释放该正常交换的接纳名额。客户端在观察服务端 FIN 后才发布远端完成；未读业务块仍可由 Stream 消费。

单流完整关闭不代表最后的数据已经可靠到达。正常客户端 Close 会取消未完成调用、等待自身 reader/回调退出，再在单独控制流上发送 `SESSION_BARRIER`。该屏障封闭父会话的新接纳，等该父会话已有接纳归零后回显随机标识。只有得到回显才记录 CloseConfirmed，再关闭物理会话。屏障超时也会回收连接，CloseConfirmed 为 false；没有业务重放。

这个流程处理了本轮真实复现的 drain 和立即关闭丢 FIN 竞争。它不证明数据库提交、应用消费或 exactly-once。服务器 ACK 已收到但交换结束未确认的情况单独计为 IncompleteExchanges，Drain 会报告 ErrDelivery。

Bad frame、第二个 OPEN、错误回执只会重置对应原生流；父 session 的拥塞、连接流量窗口、CPU 和失败仍然共享。基础行为依据 [quic-go 流文档](https://quic-go.net/docs/quic/streams/) 与 [连接关闭文档](https://quic-go.net/docs/quic/connection/)，不是自行发明 QUIC。

## 注册发现的契约

```go
registry, err := discovery.New(ctx, discovery.Config{
    Endpoints: []string{"http://127.0.0.1:2379"},
    Namespace: "/kanarpc/v2/lab",
})
// Check errors; registry.Close(cleanupCtx) joins owned workers.
registration, err := registry.Register(ctx, "analysis", kanarpc.Endpoint{
    ID: "worker-1", Address: address, Transport: kanarpc.QUIC,
}, 5)
resolver, err := registry.Watch(ctx, "analysis", kanarpc.QUIC)
endpoints, err := resolver.Endpoints() // fails closed when stale; may be empty
// Choose an explicit endpoint and DialEndpoint; no business retry is implied.
```

key 为 `Namespace/services/service/instanceID`，value 是 version=2 的类型化 endpoint。没有旧发现目录兼容或隐式降级。注册初次只允许 CreateRevision/Version=0；恢复只允许原 lease 和内容仍匹配，或键仍不存在。每次续租确认后做线性一致 read-back，检查 lease 和内容后更新本地就绪观察；他人替换不会被覆盖或撤销。外部调用者不得把其他键附到本句柄拥有的 lease 上；lease 不是权限凭据。

Ready 是有期限的本地观察，不保证服务此刻健康，也不是每次读取都访问 etcd。续租检查额外消耗控制面 RPC。句柄退休取消自身 Context；Close 返回撤销错误时，记录可能仍需等 lease TTL 到期，不能宣称已经移除。Registry 保留首次 cleanup 错误和累计次数，不无限保存错误历史。

Resolver 先线性一致 Get，再从 R+1 watch。一个事务同 revision 的多个事件一起应用；Created 头不能跳过待重放事件或刷新新鲜度。只有新快照和 watch progress barrier 刷新 FreshnessTTL，历史事件本身不延长就绪；过期时 Endpoints 返回 ErrNotReady 并隐藏地址。断流/compaction 后失效并完整重建；cluster ID 改变不沿用旧快照。Readiness 不等于 quorum、后端存活或跨机同时停止保证。相关语义见 [etcd API guarantees](https://etcd.io/docs/v3.6/learning/api_guarantees/)。

每 Registry 默认上限 64 个注册、64 个 resolver、每服务 128 个记录；单条元数据 4 KiB，gRPC 接收消息上限 4 MiB。当前没有发现驱动连接池、自动换址重试或自动把 Server.Ready 发布为 endpoint 健康状态。上层需要明确安排撤销注册、Drain 与连接选择。

## 可复现入口

```sh
python3 scripts/verify-discovery.py --output /tmp/r7b-real-etcd --race
python3 scripts/verify-discovery.py --output /tmp/r7b-min --toolchain go1.26.0 --race
python3 scripts/verify-transports.py --output /tmp/r7b-process-controls
```

第一个入口要求本机 etcd 可执行文件，会创建独立数据目录、随机 loopback client/peer 端口并回收它拥有的进程；不借用或停止既有服务。第二个验证最低 Go。第三个是串行、两进程、封闭并发的本地 JSON 负载，有 64 次 warmup、每种组合三次重复、原始 offered 记录、失败和全进程 CPU。不是真实模型或生产容量。

当前旧目录和 CampusTrace 继续保留；R7C 治理与 R7D SDK/应用迁移仍未完成。最终结果见 [R7B 报告](verification/transport-r7b/report.md)。
