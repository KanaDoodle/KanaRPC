package kanarpc

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

// This benchmark isolates server DATA / client CREDIT pacing. Recv(nil) skips
// application decoding; final multi-process controls use normal JSON responses.
func BenchmarkQUICStreamingWrite(b *testing.B) {
	for _, size := range []int{128, 900, 1024, 4096, 16384} {
		b.Run(fmt.Sprint("Payload", size), func(b *testing.B) {
			payload := strings.Repeat("x", size)
			var calls atomic.Int64
			s, c, _ := startQUIC(b, ServerConfig{MaxPerConnection: 64}, QUICConfig{}, func(s *Server) {
				require(b, RegisterStream(s, "/bench/items", func(_ context.Context, n int, send func(string) error) error {
					calls.Add(1)
					for i := 0; i < n; i++ {
						if e := send(payload); e != nil {
							return e
						}
					}
					return nil
				}))
			}, ClientConfig{StreamWindow: 8, MaxInFlight: 32})
			call := func() error {
				stream, e := c.Stream(context.Background(), "/bench/items", 16)
				if e != nil {
					return e
				}
				defer stream.Close()
				for i := 0; i < 16; i++ {
					if e = stream.Recv(nil); e != nil {
						return e
					}
				}
				if e = stream.Recv(nil); e != io.EOF {
					return fmt.Errorf("terminal: %v", e)
				}
				return nil
			}
			for i := 0; i < 16; i++ {
				require(b, call())
			}
			start := c.conn.ConnectionStats()
			b.SetParallelism(4)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if e := call(); e != nil {
						b.Error(e)
						return
					}
				}
			})
			b.StopTimer()
			end := c.conn.ConnectionStats()
			b.ReportMetric(float64(end.PacketsSent-start.PacketsSent+end.PacketsReceived-start.PacketsReceived)/float64(b.N), "packets/RPC")
			b.ReportMetric(float64(end.BytesSent-start.BytesSent+end.BytesReceived-start.BytesReceived)/float64(b.N), "wireB/RPC")
			if calls.Load() != int64(b.N+16) {
				b.Fatal("unexpected business execution count", calls.Load(), b.N)
			}
			eventually(b, func() bool { return s.Stats().InFlight == 0 && c.AdmissionStats().Active == 0 })
			if c.MemoryStats().Receive.Used != 0 {
				b.Fatal("unread budget retained", c.MemoryStats())
			}
		})
	}
}
