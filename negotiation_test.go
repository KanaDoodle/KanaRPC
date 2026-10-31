package kanarpc

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestProtobufRequiresNegotiationBeforeEncoding(t *testing.T) {
	left, right := net.Pipe()
	c, err := NewClient(left, ClientConfig{})
	require(t, err)
	defer c.Close()
	defer right.Close()
	var count atomic.Int64
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err = c.Call(ctx, "/proto/echo", Protobuf(&countedProto{wrapperspb.String("hello"), &count}), new(wrapperspb.StringValue))
	if !errors.Is(err, ErrCapabilities) || count.Load() != 0 || c.Stats().Started != 0 {
		t.Fatalf("unnegotiated call entered encoder/wire path: count=%d started=%d err=%v", count.Load(), c.Stats().Started, err)
	}
}

func TestNegotiationFailuresAreBoundedAndPreserveCauses(t *testing.T) {
	for _, mode := range []string{"legacy", "missing_feature", "wrong_nonce", "wrong_version", "oversized", "timeout", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			l, err := net.Listen("tcp", "127.0.0.1:0")
			require(t, err)
			defer l.Close()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pool, err := NewMemoryPool(MemoryPoolConfig{}, 1)
			require(t, err)
			account, err := pool.NewAccount()
			require(t, err)
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := l.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				_ = conn.SetDeadline(time.Now().Add(time.Second))
				f, err := wire.Read(conn, wire.HardLimit)
				if err != nil || f.Kind != wire.Hello {
					return
				}
				if mode == "legacy" {
					return
				}
				if mode == "cancel" {
					cancel()
				}
				if mode == "timeout" || mode == "cancel" {
					var one [1]byte
					_, _ = conn.Read(one[:])
					return
				}
				binary.BigEndian.PutUint32(f.Payload[16:20], uint32(supportedFeatures))
				switch mode {
				case "missing_feature":
					binary.BigEndian.PutUint32(f.Payload[16:20], uint32(FeatureServerStreaming))
				case "wrong_nonce":
					f.Payload[0] ^= 255
				case "wrong_version":
					binary.BigEndian.PutUint32(f.Payload[24:28], 2)
				case "oversized":
					binary.BigEndian.PutUint32(f.Payload[20:24], wire.HardLimit+1)
				}
				_ = wire.Write(conn, wire.Frame{Kind: wire.Welcome, ID: 1, Payload: f.Payload}, wire.HardLimit)
			}()
			client, err := Dial(ctx, l.Addr().String(), ClientConfig{MemoryAccount: account, RequiredFeatures: FeatureProtobuf, Timeout: 200 * time.Millisecond})
			if client != nil {
				client.Close()
				t.Fatal("failed negotiation returned a client")
			}
			if !errors.Is(err, ErrCapabilities) {
				t.Fatal(err)
			}
			// The creator owns final account removal; failed Dial must leave it
			// quiescent and releasable, including partial handshakes.
			require(t, account.Release())
			if stats := pool.Stats(); stats.Accounts != 0 || stats.Total.Used != 0 || stats.Created != stats.Released {
				t.Fatal("negotiation retained pool resources", stats)
			}
			if mode == "timeout" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			if mode == "cancel" && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("handshake peer retained")
			}
		})
	}
}

func TestNegotiationFrameLimitAndBusinessIsolation(t *testing.T) {
	for _, limits := range [][2]uint32{{1024, 4096}, {4096, 1024}} {
		s, address := start(t, ServerConfig{IO: IOConfig{MaxFrameBytes: limits[0]}}, func(s *Server) {
			registerProtoEcho(t, s)
			require(t, RegisterProtobufUnary(s, "/proto/large", func() *wrapperspb.StringValue { return new(wrapperspb.StringValue) }, func(context.Context, *wrapperspb.StringValue) (*wrapperspb.StringValue, error) {
				return wrapperspb.String(strings.Repeat("x", 2048)), nil
			}))
		})
		c := connect(t, address, ClientConfig{RequiredFeatures: FeatureProtobuf, IO: IOConfig{MaxFrameBytes: limits[1]}})
		if caps := c.Capabilities(); !caps.Negotiated || caps.MaxFrameBytes != 1024 || s.Stats().Admitted != 0 {
			t.Fatal(caps, s.Stats())
		}
		if c.MemoryStats().ControlReceive.Used != 0 || c.MemoryStats().ControlSend.Used != 0 {
			t.Fatal(c.MemoryStats())
		}
		if err := c.Call(context.Background(), "/proto/echo", Protobuf(wrapperspb.String(strings.Repeat("x", 1024))), new(wrapperspb.StringValue)); statusOf(err).Code != InvalidArgument {
			t.Fatal(err)
		}
		if s.Stats().Admitted != 0 {
			t.Fatal("oversized request dispatched", s.Stats())
		}
		if err := c.Call(context.Background(), "/proto/large", Protobuf(wrapperspb.String("")), new(wrapperspb.StringValue)); statusOf(err).Code != Internal {
			t.Fatal("response exceeded negotiated frame size", err)
		}
		require(t, c.Call(context.Background(), "/proto/echo", Protobuf(wrapperspb.String("ok")), new(wrapperspb.StringValue)))
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		report, err := s.DrainWithReport(ctx)
		cancel()
		require(t, err)
		if !report.Quiescent || report.TotalFaults.LostTerminals != 0 {
			t.Fatal(report)
		}
	}
}

func TestNegotiatedQUICFrameLimitPrecedesAllocation(t *testing.T) {
	s, c, _ := startQUIC(t, ServerConfig{IO: IOConfig{MaxFrameBytes: 4096}}, QUICConfig{}, func(s *Server) { registerProtoEcho(t, s) }, ClientConfig{RequiredFeatures: FeatureProtobuf, IO: IOConfig{MaxFrameBytes: 1024}})
	if c.Capabilities().MaxFrameBytes != 1024 {
		t.Fatal(c.Capabilities())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := c.conn.OpenStreamSync(ctx)
	require(t, err)
	defer stream.CancelRead(nativeCancel)
	defer stream.CancelWrite(nativeCancel)
	var header [wire.HeaderSize]byte
	copy(header[:4], "KNR2")
	header[4], header[5] = 2, byte(wire.OpenEncoded)
	binary.BigEndian.PutUint64(header[8:16], 1)
	binary.BigEndian.PutUint32(header[16:20], 2048)
	_, err = stream.Write(header[:])
	require(t, err)
	_ = stream.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	var one [1]byte
	_, readErr := stream.Read(one[:])
	stream.CancelRead(nativeCancel)
	stream.CancelWrite(nativeCancel)
	eventually(t, func() bool { return s.Stats().NativeStreams == 0 })
	if !errors.Is(nativeError(readErr), ErrProtocol) || s.MemoryStats().Receive.Peak != 0 || s.Stats().Admitted != 0 {
		t.Fatalf("oversized negotiated frame reached allocation: err=%v memory=%+v admitted=%d", readErr, s.MemoryStats(), s.Stats().Admitted)
	}
}
