module github.com/KanaDoodle/KanaRPC/tools/grpc-compare

go 1.26.10

toolchain go1.27.3

require (
	github.com/KanaDoodle/KanaRPC v0.0.0
	google.golang.org/grpc v1.85.0
	google.golang.org/protobuf v1.36.12
)

require (
	github.com/quic-go/quic-go v0.64.1 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.62.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260817212433-ac3dfec99bb1 // indirect
)

replace github.com/KanaDoodle/KanaRPC => ../..
