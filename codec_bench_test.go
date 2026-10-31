package kanarpc_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KanaDoodle/KanaRPC/examples/echo"
	"google.golang.org/protobuf/proto"
)

// These compare application bodies only, with identical schema/data. They do
// not measure network throughput, protocol metadata or application latency.
func BenchmarkPayloadEncoding(b *testing.B) {
	values := make([]int64, 256)
	for i := range values {
		values[i] = int64(i)
	}
	for _, test := range []struct {
		name string
		in   *echo.EchoRequest
	}{
		{"empty", &echo.EchoRequest{}},
		{"short_text", &echo.EchoRequest{Text: "hello RPC", Count: 3}},
		{"long_text", &echo.EchoRequest{Text: strings.Repeat("rpc", 1024)}},
		{"numbers", &echo.EchoRequest{Values: values}},
	} {
		b.Run(test.name, func(b *testing.B) {
			for _, format := range []string{"json", "protobuf"} {
				b.Run(format, func(b *testing.B) {
					encode := func() ([]byte, error) { return json.Marshal(test.in) }
					if format == "protobuf" {
						encode = func() ([]byte, error) {
							_ = proto.Size(test.in) // Same preflight used by RPC admission.
							return proto.Marshal(test.in)
						}
					}
					body, err := encode()
					if err != nil {
						b.Fatal(err)
					}
					b.Run("encode", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							if _, err := encode(); err != nil {
								b.Fatal(err)
							}
						}
						b.ReportMetric(float64(len(body)), "body-bytes")
					})
					b.Run("decode", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							out := new(echo.EchoRequest)
							var err error
							if format == "protobuf" {
								err = (proto.UnmarshalOptions{RecursionLimit: 64}).Unmarshal(body, out)
							} else {
								err = json.Unmarshal(body, out)
							}
							if err != nil {
								b.Fatal(err)
							}
						}
					})
				})
			}
		})
	}
}
