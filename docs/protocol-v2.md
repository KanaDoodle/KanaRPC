# KNR2 实验协议

协议版本 2，当前仅用于明确配置的有序字节流。与 KanaRPC-Go 的 `0x1234` 帧不兼容，不尝试协商降级，也不重发已发送业务。

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

帧 payload 默认上限 1 MiB，可配置 1 KiB..16 MiB；包括 OPEN 的元数据。读取器在分配 payload 前验证头、长度和记录类型。没有压缩、codec 协商或业务重试。

| kind | number / payload | 方向与约束 |
| --- | --- | --- |
| 1 OPEN | number=0，JSON `{method,budget_ns,window,stream,request}` | 客户端发起；ID 按实际 OPEN 写入顺序严格递增，允许未发送调用留下 ID 间隔 |
| 2 DATA | number 从 1 递增；JSON 业务块 | 服务端响应；发送前消耗一份信用 |
| 3 END | number=0；JSON `{code,message?,receipt?,pressure?}` | 服务端终态；正常已接纳调用使用随机 128 bit receipt 的十六进制表示；压力元数据可选 |
| 4 CANCEL | number=0，无 payload | 客户端取消；旧 ID 的迟到控制可忽略，未来 ID 为协议错误 |
| 5 CREDIT | number=1，无 payload | 每消费一个块返还一份信用；不能返还尚未发送的块 |
| 6 ACK | number=0，16 字节原始 receipt | 客户端收到终态标识后确认；过早或错误 receipt 关闭该字节流，重复正确 ACK 幂等 |
| 7 SESSION_BARRIER | number=0，16 字节随机标识 | 仅原生 QUIC 控制流的首帧；封闭父会话接纳并等待已有交换退休后回显，不能作为业务请求 |

method 为 `/service/method`，总长不超过 128 字节，段内只允许 ASCII 字母数字、点、横线和下划线。初始信用 1..32；普通调用必须为 1，业务 DATA 必须恰好一个，然后 END。流式调用允许零到多个 DATA。

## 预算、取消和送达

客户端配置预算从编码前开始，覆盖写锁等待、写入、接收与 ACK；调用者 Context 更早到期时优先。无法抢占用户编码函数，编码返回后若预算已耗尽不再发送。OPEN 内 `budget_ns` 是写入前的剩余相对预算，服务端取它与本地最大执行时间的较小值。它避免依赖跨机绝对时钟，但不扣除网络飞行时间；不是分布式同时截止保证。

本地取消先让调用返回，再以独立短控制预算发 CANCEL；客户端仍用同一 writer 保证 OPEN/CANCEL 次序。控制发送和 socket 写超时有上限，失败且可能部分写入时关闭连接以防帧错位。正常 END 与 credit 写入协调，防止正常完成取消一个正在写入的 credit。

远端错误允许已经到达的 DATA 先被消费；本地取消或传输失败清空尚在途请求的未读缓冲。过期请求的响应按 ID 丢弃，不匹配新调用。客户端 ACK 后才发布远端完成，但 ACK 仍只表示协议终态被收到，不是应用已消费所有块或业务已提交。

未接纳拒绝及服务端预算耗尽的错误终态可以没有 receipt，不占额外 ACK 等待者。正常已接纳结果在 ACK、取消或 ACK 期限后退休；缺少非取消结果的 ACK 会保留为 `LostTerminals`，随后 Drain 返回 `ErrDelivery`。这个统计是服务端整个生命周期累计值。

R7B 对每个原生 QUIC 交换只允许一个 OPEN。客户端 END/ACK 后等服务端 FIN，先发送自身 FIN 再发布完成；服务端观察客户端 FIN 后退休正常交换。客户端 Close 通过 SESSION_BARRIER 等待父会话 quiescence 后关闭连接；超时仍强制回收并记录未确认。完整约束见 [transport-r7b.md](transport-r7b.md)。

写入将头和正文合并成一个临时记录缓冲，对 TCP/TLS/QUIC 一致；处理部分写入和零进展，增加最多一帧的临时复制。R7C 在分配正文前预留共享接收字节，在复制发送记录前预留共享发送字节；控制记录使用独立预算。OPEN 保守按正文两倍计费，已结束未读流继续计费。预算范围与不覆盖的分配见 [治理说明](governance-r7c.md)，不是零复制或 RSS 上限。

R7C 的 `pressure` 使用 v=1、128 bit epoch 十六进制、递增 seq、ttl_ms、method、执行/排队/有需求物理连接数及建议并发。旧客户端可忽略该 JSON 字段；新客户端仅在显式启用 Feedback 时使用，路径/版本/epoch/序列/数值边界及保守 TTL 校验失败均回退本地静态限额。它不是签名、分布式配额或重试指令，不需要新增帧类型。

END 编码后同时受配置帧上限和 4096 字节控制正文上限约束。错误消息即使原始 UTF-8 字节不超过 256，也可能在 JSON 转义后放大；因此发送前按有效 UTF-8 边界缩短可选 message，必要时丢弃可选 pressure，保留 code 和 receipt。接收方不能假设收到完整错误诊断文本，业务状态不依赖 message 内容。

协议记录和调用方 JSON 之间没有外部信任关系。R7B 提供显式 TLS/QUIC 认证，TCP 仍是显式的无认证传输；方法鉴权、更强抗 DoS 和租户成本归属仍需后续阶段。

使用的 Context 传播与取消模式参见 [Go 官方说明](https://go.dev/blog/context)；连接 deadline 与 Close 的语义参见 [Go net 文档](https://pkg.go.dev/net#Conn)。这些通用机制不是本项目首创。
