package kanarpc

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func startQUIC(t testing.TB, sc ServerConfig, qc QUICConfig, register func(*Server), configs ...ClientConfig) (*Server, *QUICClient, string) {
	t.Helper()
	credentials, err := testcert.New()
	require(t, err)
	s, err := NewServer(sc)
	require(t, err)
	register(s)
	l, err := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, qc)
	require(t, err)
	done := make(chan error, 1)
	go func() { done <- s.ServeQUIC(l) }()
	eventually(t, s.Ready)
	clientConfig := ClientConfig{MaxInFlight: 128, StreamWindow: 2}
	if len(configs) > 0 {
		clientConfig = configs[0]
	}
	c, err := DialQUIC(context.Background(), l.Addr().String(), TLSOptions{Config: credentials.Client}, QUICConfig{}, clientConfig)
	require(t, err)
	t.Cleanup(func() {
		c.Close()
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require(t, s.Wait(ctx))
		require(t, <-done)
	})
	return s, c, l.Addr().String()
}

func TestNativeQUICConformanceAndPhysicalSessionAccounting(t *testing.T) {
	s, c, _ := startQUIC(t, ServerConfig{MaxPerConnection: 64}, QUICConfig{}, func(s *Server) {
		echo(s)
		require(t, RegisterStream(s, "/demo/items", func(_ context.Context, n int, send func(int) error) error {
			for i := 0; i < n; i++ {
				if err := send(i); err != nil {
					return err
				}
			}
			return nil
		}))
	})
	var workers sync.WaitGroup
	for n := 0; n < 24; n++ {
		workers.Add(1)
		go func(n int) {
			defer workers.Done()
			var out int
			if err := c.Call(context.Background(), "/demo/echo", n, &out); err != nil || out != n {
				t.Error(out, err)
			}
		}(n)
	}
	stream, err := c.Stream(context.Background(), "/demo/items", 20)
	require(t, err)
	for n := 0; n < 20; n++ {
		var out int
		require(t, stream.Recv(&out))
		if out != n {
			t.Fatal(out)
		}
	}
	if err := stream.Recv(nil); err != io.EOF {
		t.Fatal(err)
	}
	workers.Wait()
	eventually(t, func() bool { return c.Stats().Active == 0 && s.Stats().InFlight == 0 && s.Stats().NativeStreams == 0 })
	if c.Stats().Opened != 25 || c.Stats().Retired != 25 || s.Stats().Connections != 1 || s.Stats().Delivered != 25 {
		t.Fatal(c.Stats(), s.Stats())
	}
}
func TestNativeQUICCancelAndBadFrameDoNotCloseOtherExchanges(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			started, release := make(chan struct{}, 2), make(chan struct{})
			s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, func(s *Server) {
				echo(s)
				require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
					started <- struct{}{}
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case <-release:
						return n, nil
					}
				}))
			})
			done := make(chan error, 1)
			go func() {
				var out int
				e := c.Call(context.Background(), "/demo/hold", 42, &out)
				if e == nil && out != 42 {
					e = ErrProtocol
				}
				done <- e
			}()
			signal(t, started)
			if bad {
				native, err := c.conn.OpenStreamSync(context.Background())
				require(t, err)
				_ = native.SetDeadline(time.Now().Add(time.Second))
				_, err = native.Write([]byte("invalid record header!!!"))
				require(t, err)
				var b [1]byte
				if _, err = native.Read(b[:]); err == nil {
					t.Fatal("bad frame was accepted")
				}
				native.CancelRead(nativeCancel)
				native.CancelWrite(nativeCancel)
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				canceled := make(chan error, 1)
				go func() { var out int; canceled <- c.Call(ctx, "/demo/hold", 7, &out) }()
				signal(t, started)
				if s.Stats().Connections != 1 || s.Stats().InFlight != 2 {
					t.Fatal(s.Stats())
				}
				cancel()
				if err := <-canceled; !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			var out int
			require(t, c.Call(context.Background(), "/demo/echo", 9, &out))
			if out != 9 {
				t.Fatal(out)
			}
			close(release)
			require(t, <-done)
			eventually(t, func() bool { return c.Stats().Active == 0 && s.Stats().InFlight == 0 && s.Stats().NativeStreams == 0 })
			if s.Stats().Connections != 1 {
				t.Fatal("physical session changed", s.Stats())
			}
		})
	}
}
func TestNativeQUICDrainDeliversAndReleasesUDPPort(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, c, address := startQUIC(t, ServerConfig{}, QUICConfig{}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
			close(started)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-release:
				return n, nil
			}
		}))
	})
	result := make(chan error, 1)
	go func() { var n int; result <- c.Call(context.Background(), "/demo/hold", 8, &n) }()
	signal(t, started)
	drain := make(chan error, 1)
	go func() {
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		drain <- s.Drain(ctx)
	}()
	eventually(t, func() bool { return !s.Ready() })
	close(release)
	require(t, <-result)
	require(t, <-drain)
	packet, err := net.ListenPacket("udp", address)
	require(t, err)
	_ = packet.Close()
	if stats := s.Stats(); stats.InFlight != 0 || stats.NativeStreams != 0 || stats.Connections != 0 || stats.Delivered != 1 {
		t.Fatal(stats)
	}
}
func TestNativeQUICStreamMustContainOnlyOneRequest(t *testing.T) {
	started := make(chan struct{})
	_, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) { close(started); <-ctx.Done(); return 0, ctx.Err() }))
	})
	stream, err := c.conn.OpenStreamSync(context.Background())
	require(t, err)
	_ = stream.SetDeadline(time.Now().Add(time.Second))
	b, _ := json.Marshal(openRecord{Method: "/demo/hold", BudgetNanos: int64(time.Second), Window: 1, Request: json.RawMessage("0")})
	require(t, wire.Write(stream, wire.Frame{Kind: wire.Open, ID: 1, Payload: b}, wire.HardLimit))
	signal(t, started)
	require(t, wire.Write(stream, wire.Frame{Kind: wire.Open, ID: 2, Payload: b}, wire.HardLimit))
	var one [1]byte
	if _, err := stream.Read(one[:]); err == nil {
		t.Fatal("second request was accepted")
	}
	stream.CancelRead(nativeCancel)
	stream.CancelWrite(nativeCancel)
}
func TestNativeQUICFirstRecordTimeout(t *testing.T) {
	s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{FirstRecordTimeout: 30 * time.Millisecond}, echo)
	stream, err := c.conn.OpenStreamSync(context.Background())
	require(t, err)
	_, err = stream.Write([]byte("K"))
	require(t, err)
	eventually(t, func() bool { return s.Stats().NativeStreams == 1 })
	time.Sleep(100 * time.Millisecond)
	if s.Stats().NativeStreams != 0 {
		t.Fatal("partial first header retained stream beyond first-record deadline", s.Stats())
	}
	stream.CancelRead(nativeCancel)
	stream.CancelWrite(nativeCancel)
}
func TestNativeQUICPerSessionCapacityAndClientLimit(t *testing.T) {
	started := make(chan struct{})
	s, c, _ := startQUIC(t, ServerConfig{MaxPerConnection: 1}, QUICConfig{}, func(s *Server) {
		echo(s)
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) { close(started); <-ctx.Done(); return 0, ctx.Err() }))
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(ctx, "/demo/hold", 1, &out) }()
	signal(t, started)
	var out int
	var status *Status
	if err := c.Call(context.Background(), "/demo/echo", 2, &out); !errors.As(err, &status) || status.Code != ResourceExhausted {
		t.Fatal(err)
	}
	c.mu.Lock()
	c.cfg.MaxInFlight = 1
	c.mu.Unlock() // no concurrent reads except exchange under c.mu
	if err := c.Call(context.Background(), "/demo/echo", 2, &out); !errors.As(err, &status) || status.Code != ResourceExhausted {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	eventually(t, func() bool { return s.Stats().InFlight == 0 && c.Stats().Active == 0 })
}
func TestNativeQUICReceiptACKBeforeFIN(t *testing.T) {
	s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, echo)
	stream, err := c.conn.OpenStreamSync(context.Background())
	require(t, err)
	_ = stream.SetDeadline(time.Now().Add(time.Second))
	b, _ := json.Marshal(openRecord{Method: "/demo/echo", BudgetNanos: int64(time.Second), Window: 1, Request: json.RawMessage("1")})
	require(t, wire.Write(stream, wire.Frame{Kind: wire.Open, ID: 1, Payload: b}, wire.HardLimit))
	var end endRecord
	for {
		f, err := wire.Read(stream, wire.HardLimit)
		require(t, err)
		if f.Kind == wire.End {
			require(t, json.Unmarshal(f.Payload, &end))
			break
		}
	}
	if s.Stats().InFlight != 1 {
		t.Fatal("retired before receipt", s.Stats())
	}
	receipt, err := hex.DecodeString(end.Receipt)
	require(t, err)
	require(t, wire.Write(stream, wire.Frame{Kind: wire.Ack, ID: 1, Payload: receipt}, wire.HardLimit))
	var one [1]byte
	if n, err := stream.Read(one[:]); n != 0 || err != io.EOF {
		t.Fatal(n, err)
	}
	_ = stream.Close()
	stream.CancelRead(nativeCancel)
	eventually(t, func() bool { return s.Stats().Delivered == 1 && s.Stats().InFlight == 0 })
}

func TestNativeQUICImmediateClientClosePreservesSuccessfulExchange(t *testing.T) {
	for i := 0; i < 40; i++ {
		s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, echo)
		var out int
		require(t, c.Call(context.Background(), "/demo/echo", i, &out))
		c.Close()
		eventually(t, func() bool { return s.Stats().InFlight == 0 })
		if stats := s.Stats(); stats.IncompleteExchanges != 0 || stats.LostTerminals != 0 {
			t.Fatalf("successful exchange was abandoned by normal client close, round %d: %+v", i, stats)
		}
		// Release each listener now, rather than retaining forty full fixtures.
		s.Close()
		ctx, stop := context.WithTimeout(context.Background(), time.Second)
		err := s.Wait(ctx)
		stop()
		require(t, err)
	}
}
func TestNativeQUICVerifiedTrustAndMutualTLS(t *testing.T) {
	credentials, err := testcert.New()
	require(t, err)
	credentials.Server.ClientAuth = tls.RequireAndVerifyClientCert
	l, err := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{})
	require(t, err)
	s, err := NewServer(ServerConfig{})
	require(t, err)
	echo(s)
	done := make(chan error, 1)
	go func() { done <- s.ServeQUIC(l) }()
	eventually(t, s.Ready)
	defer func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require(t, s.Wait(ctx))
		require(t, <-done)
	}()
	for _, kind := range []string{"good", "hostname", "ca", "missing-cert"} {
		config := credentials.Client.Clone()
		switch kind {
		case "hostname":
			config.ServerName = "wrong"
		case "ca":
			other, e := testcert.New()
			require(t, e)
			config.RootCAs = other.Client.RootCAs
		case "missing-cert":
			config.Certificates = nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		c, err := DialQUIC(ctx, l.Addr().String(), TLSOptions{Config: config}, QUICConfig{}, ClientConfig{})
		if err == nil {
			var out int
			err = c.Call(ctx, "/demo/echo", 3, &out)
			c.Close()
		}
		cancel()
		if kind == "good" {
			require(t, err)
		} else if err == nil {
			t.Fatal("accepted invalid trust", kind)
		}
	}
}

func TestNativeQUICPhysicalSessionCapacityIsStructured(t *testing.T) {
	credentials, err := testcert.New()
	require(t, err)
	s, err := NewServer(ServerConfig{MaxConnections: 1})
	require(t, err)
	echo(s)
	l, err := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{})
	require(t, err)
	done := make(chan error, 1)
	go func() { done <- s.ServeQUIC(l) }()
	eventually(t, s.Ready)
	t.Cleanup(func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require(t, s.Wait(ctx))
		require(t, <-done)
	})
	first, err := DialQUIC(context.Background(), l.Addr().String(), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{})
	require(t, err)
	defer first.Close()
	eventually(t, func() bool { return s.Stats().Connections == 1 })
	second, err := DialQUIC(context.Background(), l.Addr().String(), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{})
	var out int
	if err == nil {
		err = second.Call(context.Background(), "/demo/echo", 1, &out)
		second.Close()
	}
	var status *Status
	if !errors.As(err, &status) || status.Code != ResourceExhausted {
		t.Fatal("session rejection lost resource status", err)
	}
	require(t, first.Call(context.Background(), "/demo/echo", 42, &out))
	if out != 42 {
		t.Fatal(out)
	}
}
func TestNativeQUICStreamSlotWaitHonorsCancellation(t *testing.T) {
	started := make(chan struct{})
	s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{MaxStreams: 1}, func(s *Server) {
		echo(s)
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) { close(started); <-ctx.Done(); return 0, ctx.Err() }))
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(ctx, "/demo/hold", 1, &out) }()
	signal(t, started)
	wait, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer stop()
	var out int
	if err := c.Call(wait, "/demo/echo", 2, &out); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	eventually(t, func() bool { return c.Stats().Active == 0 && s.Stats().InFlight == 0 })
	require(t, c.Call(context.Background(), "/demo/echo", 3, &out))
}
func TestNativeQUICMissingReceiptACKIsBoundedAndVisible(t *testing.T) {
	s, c, _ := startQUIC(t, ServerConfig{AckTimeout: 30 * time.Millisecond}, QUICConfig{}, echo)
	stream, err := c.conn.OpenStreamSync(context.Background())
	require(t, err)
	_ = stream.SetDeadline(time.Now().Add(time.Second))
	b, _ := json.Marshal(openRecord{Method: "/demo/echo", BudgetNanos: int64(time.Second), Window: 1, Request: json.RawMessage("1")})
	require(t, wire.Write(stream, wire.Frame{Kind: wire.Open, ID: 1, Payload: b}, wire.HardLimit))
	for {
		frame, err := wire.Read(stream, wire.HardLimit)
		require(t, err)
		if frame.Kind == wire.End {
			break
		}
	}
	eventually(t, func() bool { return s.Stats().InFlight == 0 })
	stream.CancelRead(nativeCancel)
	stream.CancelWrite(nativeCancel)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.Drain(ctx); !errors.Is(err, ErrDelivery) {
		t.Fatal("missing receipt was hidden", err)
	}
}
