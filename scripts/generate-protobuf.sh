#!/bin/sh
set -eu
export GOWORK=off

cd "$(dirname "$0")/.."
proto_compiler=${PROTOC:-protoc}
tool_dir=$(mktemp -d "${TMPDIR:-/tmp}/kanarpc-codegen.XXXXXX")
trap 'rm -rf "$tool_dir"' EXIT HUP INT TERM

# Keep generated messages and the module runtime on the same reviewed version.
protobuf_version=$(GOWORK=off go list -m -f '{{.Version}}' google.golang.org/protobuf)
GOBIN="$tool_dir" GOWORK=off go install "google.golang.org/protobuf/cmd/protoc-gen-go@$protobuf_version"
go build -o "$tool_dir/protoc-gen-kanarpc" ./cmd/protoc-gen-kanarpc
"$proto_compiler" \
  --plugin="protoc-gen-go=$tool_dir/protoc-gen-go" \
  --plugin="protoc-gen-kanarpc=$tool_dir/protoc-gen-kanarpc" \
  --go_out=. --go_opt=paths=source_relative \
  --kanarpc_out=. --kanarpc_opt=paths=source_relative \
  examples/echo/echo.proto
