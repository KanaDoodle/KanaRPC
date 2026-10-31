# Schema 发布基线

`schema.binpb` 是通过 Buf v1.75.0 生成的 FileDescriptorSet，记录首个 SDK schema。平常重新生成 Go 代码不会覆盖它。

```sh
python3 scripts/check-schema.py
```

检查分别报告二进制 WIRE、JSON WIRE_JSON 和生成源码 FILE 规则。默认基线适合本地开发；CI 对 PR 使用目标分支提交中的基线，发布时使用上一个版本 tag 的基线，避免同一次变更更新基线而掩盖破坏。

兼容变更通过评审后，可为下一次版本发布生成新的候选镜像，再与 schema 一起审查：

```sh
buf build --as-file-descriptor-set -o /tmp/schema-candidate.binpb
```

字段名或编号的重用、删除、类型/presence 和流方向变化需要明确的兼容策略。新增 RPC 可由生成的 `Unimplemented<Service>Server` 提供默认实现；业务服务须按值嵌入该类型。规则范围参见 [Buf 官方说明](https://buf.build/docs/breaking/rules/)。
