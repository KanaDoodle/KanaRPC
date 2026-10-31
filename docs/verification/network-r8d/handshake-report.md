# R8D D1/D2：定位并修复本轮复现的 QUIC 建连超时

2026-10-31，本机 darwin/arm64、Apple M5，Go 1.27.1，quic-go v0.64.1。修复前运行时基线为 R8C `5b034ad`，诊断代码另行注入；[源码快照](baseline-source.tar.gz)与[文件 hash](baseline-source-sha256.json)保留。没有通过提高 timeout、重试或删除失败样本让结果变绿。

## 自然复现与根因

同一真实 RPC listener/TLS 配置，分别运行纯 `quic.DialAddr`/Close 和公开 RPC Dial/Call/Close，各 5,000 次。两种模式各捕获 1 次 dial 阶段超时，均未进入 RPC 接纳；[完整日志](diagnostic-first.log)、[失败双端 qlog 与成功邻居](handshake-traces.tar.gz)保留。

| 自然失败 | 客户端端口 / 服务端端口 | 包级证据 |
| --- | --- | --- |
| native，第 3720 次（从 0 起） | 61991 / 56602 | 客户端发送 8 个 Initial、收到 0 包；服务端收到 8 个 Initial、发出 16 包后超时；目标回复端口当时已有无关本机进程的 IPv4 wildcard UDP socket。未停止或读取该进程的应用流量 |
| RPC，第 140 次（从 0 起） | 56602 / 56602 | 客户端的双栈 wildcard 临时端口恰与 IPv4 服务端口相同；服务端收到 Initial 后回复却落回 IPv4 listener，出现对自己响应包的解析错误；客户端始终收不到响应 |

固定版本的 `DialAddr` 先 `ListenUDP("udp", IPv4zero:0)`，然后才解析目标地址。Go 在本机将该 wildcard 变成双栈 IPv6 socket；Darwin 的 **port=0 自动端口分配** 在本次环境中可选到已被 IPv4 socket 占用的端口。发往 IPv4 目标时源地址呈 127.0.0.1:P，回复却优先交给已有的 IPv4 socket，导致握手 Initial/PTO 一直重发直到 3s 预算耗尽。

这是本机实际捕获和受控复现确认的机制，不是推测为 TLS 慢、池锁、服务端容量或用户取消。不能据此把 R8C 历史每次没有包级记录的超时全部追溯归为同一原因，也不宣称解决了所有 QUIC 超时。

```mermaid
flowchart LR
    C[旧客户端 双栈 wildcard :P] -->|IPv4 Initial| S[IPv4 QUIC 服务端]
    S -->|回复 127.0.0.1:P| I[已有 IPv4 socket :P]
    I --> D[客户端未收到回复]
    D --> T[重传至 3s deadline]
```

## 受控复现与失败夹具

新测试仅创建自己拥有的 64 个 IPv4 UDP sockets，在至多 8192 次旧式临时端口分配中取得一个交叉地址族冲突对，随后确定性验证：

1. 双栈 socket 发 request，IPv4 echo 发 reply，reply 实际由冲突的 IPv4 sink 收到，双栈 socket 的 read 超时。
2. 用同样的 socket 调用纯 quic-go，服务端的 Initial 实际进入自有 sink，握手超时且 RPC Admitted=0。
3. 公开 RPC 的 IPv4 目标必须使用 IPv4 客户端 socket；修复前该 fence 失败，修复后成功并正常返回 echo。

[修复前](collision-acquired-before.jsonl)、[修复后 race 重复](collision-after.jsonl)。取得碰撞对的搜索有明确上限；真正的转交和握手失败断言在取得对之后固定重现。

第一次夹具尝试直接 bind 一个已占用的固定端口，得到了正确的 EADDRINUSE，未能模拟 port=0 的内核选择路径；[原记录](collision-before.jsonl)保留。这是夹具错误，不计为框架漏洞；没有改 OS 端口范围或关闭用户服务来制造复现。

## 最小修复与所有权

- 在握手 deadline 内解析地址，按实际目标选择 `udp4` / `udp6`，再将显式 socket 交给 `quic.Dial`。解析仍保持原来的 IPv4 优先选择，不做自动重试、地址回退或 TCP 降级。
- 保留原 hostname 作为 TLS ServerName，避免先解析成 IP 后破坏原来的 DNS 名称认证；显式配置的 ServerName 优先。
- 客户端显式拥有该 UDP socket，建连失败、父连接远端终止和公开 Close 都回收；原生请求流关闭不关闭父 socket。原来的 close barrier、ACK/FIN、取消、压力反馈与信用协议保持不变。
- 地址解析预算、IPv4/IPv6/映射 IPv4、hostname 推断、用户取消身份、失败建连 FD、远端关闭和重复 Close 均补回归：[记录](address-lifecycle-after.jsonl)。

选择显式 socket 和其关闭责任遵循 quic-go 的[客户端接口](https://quic-go.net/docs/quic/client/)与[Transport 所有权说明](https://quic-go.net/docs/quic/transport/)；具体旧快捷函数实现以[固定 v0.64.1 源码](https://github.com/quic-go/quic-go/blob/v0.64.1/client.go)及本机 module cache 为准。外部文档不用于替代上面的故障实测。

## 新旧对照与边界

| 对照 | 旧基线 | 修复后 | 解释 |
| --- | --- | --- | --- |
| 普通背景，公开 RPC，各计划 20,000 次 | 20,000 实际尝试，0 失败 | 20,000 实际尝试，0 失败 | [旧](rpc-churn-baseline.log)、[新](rpc-churn-fixed.log)；保留这个“未观察到收益”的结果，不能据此计算修复降低了失败率或提高了吞吐 |
| 自有 64 个 IPv4 端口占用，公开 RPC，各计划 5,000 次 | 1,489 实际尝试后达到 3 次失败停止；剩余 3,511 未启动 | 5,000 实际尝试，0 失败 | [旧](occupied-baseline.log)、[新](occupied-fixed.log)；同诊断器、预算与背景占用，停止规则相同；不是 5,000 次旧请求全部已测 |
| Linux VM，纯 quic-go 快捷函数 | 5,000 次，0 失败；增加 64 个自有 IPv4 占用后也 5,000 次、0 失败 | 不用于推导 Mac 失败率 | [普通](linux-native-control.log)、[占用](linux-occupied-native.log)；同本地 Docker VM，不是另一台物理机器 |

普通背景样本本身可能完全错过稀有故障。证明此次修复针对该机制的主要证据是双端包迹、自有 socket 的受控转交，以及地址族 fence 和生命周期回归；不是单凭跑出零失败。

真实双机、真实网卡、其他 Darwin 版本和小时/天级可靠性尚未验证；服务端 wildcard/双栈绑定策略不由这次客户端修复统一改写。网络引入的合法丢包、对端故障和 deadline 仍会产生建连失败，后续验证见主报告。
