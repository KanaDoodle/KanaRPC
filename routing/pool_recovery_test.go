package routing

import (
	"context"
	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPoolQUICSaturationRecoveryReproduction(t *testing.T) { poolQUICResetRecovery(t, 16384, 24576) }
func TestPoolQUICSmallRecordResetRecovery(t *testing.T)       { poolQUICResetRecovery(t, 900, 1024) }
func poolQUICResetRecovery(t *testing.T, payloadBytes int, receiveBytes int64) {
	credentials, err := testcert.New()
	check(t, err)
	type request struct {
		Payload string
		N       int
	}
	register := func(s *rpc.Server) {
		check(t, rpc.RegisterUnary(s, "/pressure/echo", func(_ context.Context, in request) (request, error) { return in, nil }))
		check(t, rpc.RegisterStream(s, "/pressure/items", func(_ context.Context, in request, send func(request) error) error {
			for i := 0; i < in.N; i++ {
				if e := send(in); e != nil {
					return e
				}
			}
			return nil
		}))
		check(t, rpc.RegisterStream(s, "/pressure/cancel", func(ctx context.Context, in request, send func(request) error) error {
			if e := send(in); e != nil {
				return e
			}
			<-ctx.Done()
			return ctx.Err()
		}))
	}
	a, b := serve(t, rpc.QUIC, credentials, 10, register), serve(t, rpc.QUIC, credentials, 20, register)
	a.node.ID = "a"
	b.node.ID = "b"
	cfg := Config{Transport: rpc.QUIC, Dial: options(credentials), MaxConnections: 2, MaxInFlight: 8}
	cfg.Dial.Client.Timeout = 3 * time.Second
	cfg.Dial.Client.IO.MaxFrameBytes = 32768
	cfg.Dial.Client.Memory.EncodingBytes = 65536
	cfg.Dial.Client.Memory.ReceiveBytes = 2 << 20
	testPoolMemory(&cfg, 65536, receiveBytes, 65536, 131072)
	source := &source{snapshot: fresh(a.node, b.node)}
	c := managed(t, source, cfg, rpc.DialEndpoint)
	stop := time.Now().Add(3 * time.Second)
	var ids atomic.Int64
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				id := ids.Add(1)
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				in := request{strings.Repeat("x", payloadBytes), 4}
				if id%4 == 0 {
					_ = c.Call(ctx, "/pressure/echo", in, new(request))
				} else {
					method := "/pressure/items"
					if id%4 == 1 {
						method = "/pressure/cancel"
					}
					stream, e := c.Stream(ctx, method, in)
					if e == nil {
						for {
							e = stream.Recv(nil)
							if e != nil {
								break
							}
							if id%4 == 1 {
								cancel()
							}
						}
						stream.Close()
					}
				}
				cancel()
			}
		}()
	}
	wg.Wait()
	until(t, func() bool {
		return a.s.Stats().InFlight == 0 && b.s.Stats().InFlight == 0 && a.s.Stats().NativeStreams == 0 && b.s.Stats().NativeStreams == 0 && c.MemoryStats().Total.Used == 0
	})
	root := callerFor(t, c, a.node).(*rpc.QUICClient)
	if c.MemoryStats().Memory.Receive.Rejected == 0 || root.Stats().Opened < 128 {
		t.Fatal("insufficient reset pressure", c.MemoryStats(), root.Stats())
	}
	t.Logf("before native=%+v admission=%+v pool=%+v", root.Stats(), root.AdmissionStats(), c.MemoryStats())
	snapshot := fresh(a.node)
	snapshot.Revision = 2
	source.set(snapshot)
	check(t, c.refresh(context.Background()))
	done := make(chan error, 1)
	go func() {
		stream, e := c.Stream(context.Background(), "/pressure/items", request{strings.Repeat("u", payloadBytes), 1})
		if e == nil {
			e = stream.Recv(nil)
			if e == nil {
				e = stream.Recv(nil)
				if e == io.EOF {
					e = nil
				}
			}
			stream.Close()
		}
		done <- e
	}()
	select {
	case e := <-done:
		check(t, e)
		return
	case <-time.After(100 * time.Millisecond):
		t.Logf("blocked native=%+v admission=%+v pool=%+v", root.Stats(), root.AdmissionStats(), c.MemoryStats())
	}
	if e := receive(t, done); e != nil {
		t.Fatalf("recovery error=%v native=%+v admission=%+v pool=%+v", e, root.Stats(), root.AdmissionStats(), c.MemoryStats())
	}
}
