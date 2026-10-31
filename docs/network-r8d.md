# QUIC 建连诊断与网络故障验证（R8D）

以 R8C `5b034ad` 为基线，定位 Darwin QUIC 偶发建连超时，修复地址族与 UDP socket 归属，并通过受控网络故障验证取消和关闭。阶段结果保存在 `docs/verification/network-r8d/`。

| 步骤 | 结合现有代码的实施边界 | 复现、基线与验收 |
| --- | --- | --- |
| D1 | 检查 `DialQUIC` → quic-go DialAddr → 握手 → 原生流 → SessionBarrier/Close；增加诊断工具，先不改超时或重试策略 | 保留 R8C 原始失败；逐请求 Dial、纯 quic-go、RPC、连接复用对照；记录每次尝试/阶段/错误和 qlog。用确定性故障验证诊断是否能区分丢包、握手停滞、用户取消与业务阶段 |
| D2 | 确认根因后修复对应生命周期/传输路径，保持原 timeout 与重试策略 | 保存修复前失败和源码；对应确定性用例、race、重复 churn；相同预算/负载的新旧串行对照，失败和未启动 offered 单独统计 |
| D3 | Mac 上受控 QUIC UDP 中继做确定性黑洞/恢复/取消；检测到本地 Docker Linux VM 后增加容器内 tc/netem 控制，覆盖 TCP/TLS/QUIC 的延迟、丢包、限速，另测慢消费者、断连和 deadline drain | 无故障链路为基线；记录实际转发/丢弃/队列及内核 qdisc，验证故障范围、不重放、最终资源归属。应用中继、Linux VM 内核注入和真实双机分别标注，不互相替代 |
| D4 | 复用已改为 JSONL 流式观察的 resource-lab，延长混合普通/流式/取消/未读负载；检查连接 churn、goroutine、heap、GC、FD 和计费回收 | 冻结版本/二进制；旧/新相同观察器与负载；长时资源观察不与吞吐性能混用。完整 Go/最低版本/race、fuzz、真实隔离 etcd 和独立 SDK 回归；最终串行性能对照 |

诊断按尝试记录握手、取消和业务阶段；已确认根因关联最小复现，其他偶发超时按原始结果单独保留。

验证环境为 darwin/arm64 loopback、应用层 UDP 中继及本地 Linux VM 内核 netem；三者按各自注入机制记录结果。真实双机和其他环境状态见 [使用约束](operating-notes.md)。

## 已确认的修复与接口边界

`DialQUIC` 在同一握手预算中解析目标，保留 hostname 用于 TLS 认证，然后按目标选 `udp4` / `udp6`。它显式拥有 UDP socket，并在建连失败、父连接结束和公开 Close 时回收。没有增加重试或延长默认 3s 握手期限。原因与前后证据见 [握手报告](verification/network-r8d/handshake-report.md)。

`internal/netfault` 是仅供测试的一客户端 UDP 中继，每方向一个 goroutine 和一个至多 65536 字节缓冲，可配置延迟、单次/周期丢弃、黑洞和串行限速。它终止两段 UDP 路径，不终止 QUIC/TLS；不保留 ECN/真实 PMTU，也不模拟真实拥塞拓扑。所有权和丢弃计数有独立测试。普通 TCP/TLS 的丢包测试通过 Linux VM 内核 tc/netem 实施，TCP 字节流保持完整，故障由内核链路层注入。

## 可复验入口

```sh
python3 scripts/verify-discovery.py --output /tmp/kanarpc-r8d-race --race
go test . -run='^TestQUICCollisionBlocksHandshakeBeforeRPC$' -race -count=10
KANARPC_IPV4_RESERVATIONS=64 KANARPC_CHURN_ATTEMPTS=5000 KANARPC_CHURN_MODE=rpc go test . -run='^TestQUICHandshakeChurnDiagnostic$' -count=1 -timeout=120s
go test . -run='^$' -fuzz='^FuzzQUICLiteralAddress$' -fuzztime=15s -parallel=2
python3 scripts/verify-resources.py --output /tmp/kanarpc-r8d-resources --baseline 5b034ad --duration 600 --cycles 20 --no-performance
```

碰撞复现依赖本轮观察到的 Darwin 行为；其他 OS 不以不存在该行为为失败。诊断器普通测试仅做两个 RPC dials；启用 churn 参数时可选 native/RPC，失败达到 3 次会停止该组，剩余尝试记未启动。`KANARPC_QUIC_DIAG` 可指向不存在的文件输出目录来保留 qlog/attempts；它可能产生较多文件，不用于长时 heap 观察。`QUIC_GO_LOG_LEVEL=info` 另可记录 socket/CID，均为调用者显式启用。

Linux VM 的内核验证：

```sh
docker build -t kanarpc-r8d-network:20261031 tools/network-r8d
mkdir /tmp/kanarpc-r8c-baseline
git archive 5b034ad | tar -x -C /tmp/kanarpc-r8c-baseline
python3 scripts/verify-network.py --baseline-source /tmp/kanarpc-r8c-baseline --output /tmp/kanarpc-r8d-netem
```

该入口从镜像架构选择交叉编译目标（当前已验 arm64，amd64 分支尚未实测），为每 case 新建 `--network none`、NET_ADMIN、仅改其 lo qdisc 的容器。不会修改宿主机 qdisc、路由或其他应用。记录镜像 ID、Go 源码/二进制 hash、内核/qdisc、每次 offered 和关闭结果，并回收自己的容器。预检会先核验所有 profile 语法；iproute2 6.1 不支持 netem seed，内核丢包/重排随机性明确不设种子，请求 ID 洗牌 seed 7 与它不同。

`TestNetworkCancelAndDeadlineDrainPhysicalTransports` 普通运行验证三传输取消/关闭基线；只有 Linux、PID 1、显式 `KANARPC_OWNED_NETNS=1` 和 `KANARPC_KERNEL_BLACKHOLE=1` 同时成立，才允许在专属容器中注入 100% 内核丢包。不能在宿主机设置这些变量代替容器验收。

资源历史逐样本写 JSONL，FD 数包括固定观察器文件；租约与 RSS 使用各自观测口径。环境、运行时长和退化数据汇总在 [主报告](verification/network-r8d/report.md)。
