# KNR2 v2 协议

KNR2 定义普通调用与服务端流式调用的记录格式。协议版本为 2，运行在显式配置的有序字节流上；TCP/TLS 在连接内复用记录，QUIC 每次请求使用独立原生流。旧 KanaRPC-Go `0x1234` 帧使用独立端点。

## 记录布局

24 字节固定头，整数使用大端：

| 偏移 | 长度 | 字段 |
| --- | --- | --- |
| 0 | 4 | ASCII `KNR2` |
| 4 | 1 | version = 2 |
| 5 | 1 | kind |
| 6 | 2 | reserved = 0 |
| 8 | 8 | request ID，非零 |
| 16 | 4 | payload 字节数 |
| 20 | 4 | number：DATA 序号或 CREDIT 数量 |

帧 payload 默认上限 1 MiB，可配置 1 KiB..16 MiB；包括 OPEN 的元数据。读取器在分配 payload 前验证头、长度和记录类型。JSON 为默认业务编码；Protobuf 调用使用独立的二进制记录类型，由调用入口和方法注册共同确定。

| kind | number / payload | 方向与约束 |
| --- | --- | --- |
| 1 OPEN | number=0，JSON `{method,budget_ns,window,stream,request}` | 客户端发起；ID 按实际 OPEN 写入顺序严格递增，允许未发送调用留下 ID 间隔 |
| 2 DATA | number 从 1 递增；JSON 业务块 | 服务端响应；发送前消耗一份信用 |
| 3 END | number=0；JSON `{code,message?,receipt?,pressure?}` | 服务端终态；正常已接纳调用使用随机 128 bit receipt 的十六进制表示；压力元数据可选 |
| 4 CANCEL | number=0，无 payload | 客户端取消；旧 ID 的迟到控制可忽略，未来 ID 为协议错误 |
| 5 CREDIT | number=1，无 payload | 每消费一个块返还一份信用；不能返还尚未发送的块 |
| 6 ACK | number=0，16 字节原始 receipt | 客户端收到终态标识后确认；过早或错误 receipt 关闭该字节流，重复正确 ACK 幂等 |
| 7 SESSION_BARRIER | number=0，16 字节随机标识 | 仅原生 QUIC 控制流的首帧；封闭父会话接纳并等待已有交换退休后回显，不能作为业务请求 |
| 8 OPEN_ENCODED | number=0；4 字节 metadata 长度 + JSON metadata + 二进制业务正文 | 客户端发起 Protobuf 调用；metadata 不超过 1024 字节，含 `{method,budget_ns,window,stream,codec:"protobuf"}` |
| 9 DATA_ENCODED | number 从 1 递增；二进制业务块，可为零字节 | Protobuf 响应；发送前消耗一份信用，与 JSON DATA 使用相同序号约束 |
| 10 HELLO | ID=1，number=0，32 字节能力提议 | 显式启用时在建连阶段发送，先于业务 OPEN；TCP/TLS 连接一次，QUIC 父会话一次 |
| 11 WELCOME | ID=1，number=0，32 字节能力响应 | 回显 nonce、声明能力位与选定帧上限，不占业务请求 ID 序列或处理器名额 |

method 为 `/service/method`，总长不超过 128 字节，段内只允许 ASCII 字母数字、点、横线和下划线。初始信用 1..32；普通调用必须为 1，业务 DATA 必须恰好一个，然后 END。流式调用允许零到多个 DATA。

OPEN_ENCODED 的 metadata 长度使用大端 uint32，必须非零且落在 payload 内；业务正文不经 base64 包装。服务端在方法接纳前检查编码；客户端只接受与本次调用编码一致的 DATA 类型。空 Protobuf 消息的零字节正文仍拥有帧、序号、信用和未读流生命周期。Protobuf 解码递归上限为 64。

kind 1..7 与原 JSON 格式保持不变。默认 JSON 客户端不发能力握手，继续接入旧节点。需要 Protobuf 的新客户端通过 `RequiredFeatures` 在发布连接前完成 HELLO/WELCOME；旧节点拒绝新握手时，Dial 返回能力错误，尚未编码或发送业务。框架不自动降级或重放业务请求。R11 客户端的原始 8/9 记录仍可由服务端解析。

HELLO/WELCOME 正文布局依次为：16 字节随机 nonce、uint32 能力位、uint32 帧上限、uint32 协商格式版本 1、uint32 保留零。整数使用大端；位 0 表示 Protobuf，位 1 表示服务端流。响应帧上限取双方配置的较小值，之后请求、响应及终态均受此上限约束。未知可选能力位可忽略，客户端所需能力必须全部出现；nonce、版本、保留字段或尺寸不合法时失败。

TCP/TLS 仅允许业务 OPEN 前的一次 HELLO；QUIC 使用独立控制流，父会话在已有业务流或已协商后拒绝再次协商。控制流纳入既有连接、原生流和控制字节预算，写入与等待受 Context 和超时约束。协商结果属于物理连接/会话，随连接退休失效；身份与方法授权仍使用既有安全契约。接入与 schema 演进见 [SDK 治理](sdk-r12.md)与 [Protobuf 指南](protobuf.md)。

## 预算、取消和送达

客户端配置预算从编码前开始，覆盖写锁等待、写入、接收与 ACK；调用者 Context 更早到期时优先。无法抢占用户编码函数，编码返回后若预算已耗尽不再发送。OPEN 内 `budget_ns` 是写入前的剩余相对预算，服务端取它与本地最大执行时间的较小值。相对预算避免依赖跨机绝对时钟；网络飞行时间不从服务端预算扣除，详见 [使用约束](operating-notes.md)。

本地取消先让调用返回，再以独立短控制预算发 CANCEL；客户端仍用同一 writer 保证 OPEN/CANCEL 次序。控制发送和 socket 写超时有上限，失败且可能部分写入时关闭连接以防帧错位。正常 END 与 credit 写入协调，防止正常完成取消一个正在写入的 credit。

远端错误允许已经到达的 DATA 先被消费；本地取消或传输失败清空尚在途请求的未读缓冲。过期请求的响应按 ID 丢弃，不匹配新调用。客户端发送 ACK 后才发布远端完成。ACK 确认协议终态标识，应用消费与业务提交由上层管理。

未接纳拒绝的错误终态没有 receipt，不占额外 ACK 等待者。已接纳的处理器若返回预算错误，只要请求 Context 仍有效，仍会发送带 receipt 的 ResourceExhausted END；若在读取 OPEN 前或写控制记录时预算已经不足，则可能只能关闭记录连接或 reset QUIC 交换。正常已接纳结果在 ACK、取消或 ACK 期限后退休；缺少非取消结果的 ACK 会保留为 `LostTerminals`，随后 Drain 返回 `ErrDelivery`。这个统计是服务端整个生命周期累计值。

R9C 不改变帧或 END 格式。本机字节接纳失败可用 `errors.As` 匹配 `*BudgetError`；远端只收到 Status，不能反推对方预算或 reset 原因。`ServerStats.Outcomes` 把接纳拒绝、实际收到的 CANCEL、请求截止时间、连接故障和回执结算分别累计。`Canceled` 仍保留原有的处理器状态码计数，不等于主动取消数。迟于回执结算的 ACK 不改写累计故障；结算前已观察到 ACK 时，后续取消或连接关闭不能推翻它。

`DrainWithReport` 区分 PriorFaults、AddedFaults、TotalFaults 与 Quiescent；资源退出和交付健康是两件事。累计故障不因重复 drain 或后续成功调用而清零；若 drain 同时超时/取消和存在交付故障，返回错误同时匹配 Context 错误和 ErrDelivery。各统计字段及返回值见 [关闭契约](drain-r9c.md)。

R7B 对每个原生 QUIC 交换只允许一个 OPEN。客户端 END/ACK 后等服务端 FIN，先发送自身 FIN 再发布完成；服务端观察客户端 FIN 后退休正常交换。客户端 Close 通过 SESSION_BARRIER 等待父会话 quiescence 后关闭连接；超时仍强制回收并记录未确认。完整约束见 [transport-r7b.md](transport-r7b.md)。

写入将头和正文合并成一个临时记录缓冲，对 TCP/TLS/QUIC 一致；处理部分写入和零进展，增加最多一帧的临时复制。R7C 在分配正文前预留共享接收字节，在复制发送记录前预留共享发送字节；控制记录使用独立预算。OPEN 保守按正文两倍计费，已结束未读流继续计费。具体计费范围见 [共享预算](pool-r9a.md)；进程内存观测见 [使用约束](operating-notes.md)。

R7C 的 `pressure` 使用 v=1、128 bit epoch 十六进制、递增 seq、ttl_ms、method、执行/排队/有需求物理连接数及建议并发。旧客户端可忽略该 JSON 字段；新客户端仅在显式启用 Feedback 时使用，路径/版本/epoch/序列/数值边界及保守 TTL 校验失败均回退本地静态限额。pressure 复用 END 的可选字段，客户端以校验后的短效建议参与接纳。信任与配额范围见 [使用约束](operating-notes.md)。

END 编码后同时受配置帧上限和 4096 字节控制正文上限约束。错误消息即使原始 UTF-8 字节不超过 256，也可能在 JSON 转义后放大；因此发送前按有效 UTF-8 边界缩短可选 message，必要时丢弃可选 pressure，保留 code 和 receipt。接收方不能假设收到完整错误诊断文本，业务状态不依赖 message 内容。

跨信任边界使用 TLS/QUIC 认证传输；方法鉴权、租户身份与输入验证由应用提供，部署要求见 [使用约束](operating-notes.md)。

使用的 Context 传播与取消模式参见 [Go 官方说明](https://go.dev/blog/context)；连接 deadline 与 Close 的语义参见 [Go net 文档](https://pkg.go.dev/net#Conn)。
