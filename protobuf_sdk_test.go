package kanarpc_test

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/discovery"
	"github.com/KanaDoodle/KanaRPC/examples/echo"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/routing"
)

type sdkSource struct {
	mu       sync.Mutex
	snapshot discovery.Snapshot
}

func (s *sdkSource) Snapshot() discovery.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := s.snapshot
	v.Endpoints = append([]rpc.Endpoint(nil), v.Endpoints...)
	return v
}
func (s *sdkSource) remove() {
	s.mu.Lock()
	s.snapshot.Revision++
	s.snapshot.Endpoints = nil
	s.mu.Unlock()
}

type sdkService struct {
	echo.UnimplementedEchoServiceServer
	calls atomic.Int64
}

func (s *sdkService) Echo(_ context.Context, in *echo.EchoRequest) (*echo.EchoResponse, error) {
	s.calls.Add(1)
	return &echo.EchoResponse{Text: in.GetText(), Label: in.Label}, nil
}
func (s *sdkService) Watch(ctx context.Context, in *echo.EchoRequest, send func(*echo.EchoResponse) error) error {
	for n := uint32(0); n < in.GetCount(); n++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := send(&echo.EchoResponse{Text: in.GetText(), Sequence: n}); err != nil {
			return err
		}
	}
	return nil
}

func sdkEventually(t *testing.T, f func() bool) {
	t.Helper()
	until := time.Now().Add(4 * time.Second)
	for !f() {
		if time.Now().After(until) {
			t.Fatal("SDK resource state did not converge")
		}
		time.Sleep(time.Millisecond)
	}
}
func sdkCheck(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestGeneratedProtobufSDKWithRoutingAndRetirement(t *testing.T) {
	for _, transport := range []rpc.Transport{rpc.TCP, rpc.TLS, rpc.QUIC} {
		t.Run(string(transport), func(t *testing.T) {
			s, err := rpc.NewServer(rpc.ServerConfig{})
			sdkCheck(t, err)
			implementation := new(sdkService)
			sdkCheck(t, echo.RegisterEchoServiceServer(s, implementation))
			credentials, err := testcert.New()
			sdkCheck(t, err)
			var address string
			done := make(chan error, 1)
			if transport == rpc.QUIC {
				l, e := rpc.ListenQUIC("127.0.0.1:0", rpc.TLSOptions{Config: credentials.Server}, rpc.QUICConfig{})
				sdkCheck(t, e)
				address = l.Addr().String()
				go func() { done <- s.ServeQUIC(l) }()
			} else {
				l, e := net.Listen("tcp", "127.0.0.1:0")
				sdkCheck(t, e)
				address = l.Addr().String()
				go func() {
					if transport == rpc.TLS {
						done <- s.ServeTLS(l, rpc.TLSOptions{Config: credentials.Server})
					} else {
						done <- s.Serve(l)
					}
				}()
			}
			t.Cleanup(func() {
				s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				sdkCheck(t, s.Wait(ctx))
				sdkCheck(t, <-done)
			})
			sdkEventually(t, s.Ready)
			source := &sdkSource{snapshot: discovery.Snapshot{Ready: true, Revision: 1, ClusterID: 1, Endpoints: []rpc.Endpoint{{ID: "worker", Address: address, Transport: transport}}}}
			managed, err := routing.New(context.Background(), source, routing.Config{Transport: transport, Dial: rpc.DialOptions{Client: rpc.ClientConfig{RequiredFeatures: rpc.FeatureProtobuf | rpc.FeatureServerStreaming}, TLS: rpc.TLSOptions{Config: credentials.Client}}})
			sdkCheck(t, err)
			t.Cleanup(func() {
				managed.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
				defer cancel()
				sdkCheck(t, managed.Wait(ctx))
			})
			client := echo.NewEchoServiceClient(managed)
			label := "" // Present zero value must survive binary round-trip.
			out, err := client.Echo(context.Background(), &echo.EchoRequest{Text: "hello", Label: &label})
			sdkCheck(t, err)
			if out.GetText() != "hello" || out.Label == nil || out.GetLabel() != label {
				t.Fatal(out)
			}
			before := implementation.calls.Load()
			if _, err := client.Echo(context.Background(), nil); err == nil || implementation.calls.Load() != before {
				t.Fatal("nil request executed", err)
			}
			stream, err := client.Watch(context.Background(), &echo.EchoRequest{Count: 3})
			sdkCheck(t, err)
			for n := uint32(0); n < 3; n++ {
				v, e := stream.Recv()
				sdkCheck(t, e)
				if v.GetSequence() != n {
					t.Fatal(v)
				}
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			// A zero-byte completed response still owns a routing stream reference.
			stream, err = client.Watch(context.Background(), &echo.EchoRequest{Count: 1})
			sdkCheck(t, err)
			sdkEventually(t, func() bool { return s.Stats().InFlight == 0 })
			source.remove()
			sdkEventually(t, func() bool { return managed.Stats().Retiring == 1 })
			if managed.MemoryStats().Accounts != 1 {
				t.Fatal(managed.MemoryStats())
			}
			v, err := stream.Recv()
			sdkCheck(t, err)
			if v.GetText() != "" || v.GetSequence() != 0 {
				t.Fatal(v)
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			sdkEventually(t, func() bool { return managed.MemoryStats().Accounts == 0 })
			if stats := managed.MemoryStats(); stats.Total.Used != 0 || stats.Created != stats.Released {
				t.Fatal(stats)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			report, err := s.DrainWithReport(ctx)
			sdkCheck(t, err)
			if !report.Quiescent || report.TotalFaults.LostTerminals != 0 {
				t.Fatal(report)
			}
		})
	}
}
