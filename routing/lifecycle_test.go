package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/discovery"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

type serverFixture struct {
	s                 *rpc.Server
	node              rpc.Endpoint
	executions        atomic.Int64
	started, canceled chan struct{}
}

func serve(t testing.TB, kind rpc.Transport, credentials testcert.Credentials, base int) *serverFixture {
	t.Helper()
	s, err := rpc.NewServer(rpc.ServerConfig{MaxPerConnection: 64})
	check(t, err)
	f := &serverFixture{s: s, started: make(chan struct{}, 8), canceled: make(chan struct{}, 8)}
	check(t, rpc.RegisterUnary(s, "/test/echo", func(_ context.Context, n int) (int, error) { f.executions.Add(1); return base + n, nil }))
	check(t, rpc.RegisterUnary(s, "/test/hold", func(ctx context.Context, n int) (int, error) {
		f.executions.Add(1)
		f.started <- struct{}{}
		<-ctx.Done()
		f.canceled <- struct{}{}
		return n, ctx.Err()
	}))
	check(t, rpc.RegisterStream(s, "/test/items", func(_ context.Context, n int, send func(int) error) error {
		f.executions.Add(1)
		for i := 0; i < n; i++ {
			if err := send(base + i); err != nil {
				return err
			}
		}
		return nil
	}))
	check(t, rpc.RegisterStream(s, "/test/cancel", func(ctx context.Context, n int, send func(int) error) error {
		f.executions.Add(1)
		if err := send(n); err != nil {
			return err
		}
		<-ctx.Done()
		f.canceled <- struct{}{}
		return ctx.Err()
	}))
	done := make(chan error, 1)
	if kind == rpc.QUIC {
		listener, e := rpc.ListenQUIC("127.0.0.1:0", rpc.TLSOptions{Config: credentials.Server}, rpc.QUICConfig{})
		check(t, e)
		f.node = rpc.Endpoint{ID: "node", Address: listener.Addr().String(), Transport: kind}
		go func() { done <- s.ServeQUIC(listener) }()
	} else {
		listener, e := net.Listen("tcp", "127.0.0.1:0")
		check(t, e)
		f.node = rpc.Endpoint{ID: "node", Address: listener.Addr().String(), Transport: kind}
		go func() {
			if kind == rpc.TLS {
				done <- s.ServeTLS(listener, rpc.TLSOptions{Config: credentials.Server})
			} else {
				done <- s.Serve(listener)
			}
		}()
	}
	until(t, s.Ready)
	t.Cleanup(func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		check(t, s.Wait(ctx))
		check(t, receive(t, done))
	})
	return f
}
func options(credentials testcert.Credentials) rpc.DialOptions {
	return rpc.DialOptions{TLS: rpc.TLSOptions{Config: credentials.Client}, Client: rpc.ClientConfig{MaxInFlight: 64, StreamWindow: 8}}
}
func callerFor(t testing.TB, c *Client, node rpc.Endpoint) rpc.Caller {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.active[node]
	if e == nil || e.caller == nil {
		t.Fatal("missing active endpoint")
	}
	return e.caller
}
func buffered(caller rpc.Caller) int64 {
	return caller.(interface{ MemoryStats() rpc.MemoryStats }).MemoryStats().Receive.Used
}

func TestDirectCloseBaselineDiscardsCompletedUnreadStream(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	f := serve(t, rpc.TCP, credentials, 10)
	c, err := rpc.DialEndpoint(context.Background(), f.node, options(credentials))
	check(t, err)
	defer c.Close()
	stream, err := c.Stream(context.Background(), "/test/items", 3)
	check(t, err)
	defer stream.Close()
	until(t, func() bool { return f.s.Stats().Delivered == 1 && f.s.Stats().InFlight == 0 && buffered(c) > 0 })
	c.Close()
	if buffered(c) != 0 {
		t.Fatal("root Close did not discard buffers")
	}
	if err := stream.Recv(new(int)); !errors.Is(err, rpc.ErrClosed) {
		t.Fatal("naive retirement did not reproduce lost unread data", err)
	}
}

func TestRetirementKeepsCompletedUnreadStreamAllTransports(t *testing.T) {
	for _, kind := range []rpc.Transport{rpc.TCP, rpc.TLS, rpc.QUIC} {
		t.Run(string(kind), func(t *testing.T) {
			credentials, err := testcert.New()
			check(t, err)
			old := serve(t, kind, credentials, 10)
			next := serve(t, kind, credentials, 20)
			s := &source{snapshot: fresh(old.node)}
			c := managed(t, s, Config{Transport: kind, Dial: options(credentials), MaxConnections: 2}, rpc.DialEndpoint)
			stream, err := c.Stream(context.Background(), "/test/items", 3)
			check(t, err)
			defer stream.Close()
			caller := callerFor(t, c, old.node)
			until(t, func() bool {
				return old.s.Stats().InFlight == 0 && old.s.Stats().Delivered == 1 && buffered(caller) > 0
			})
			v := fresh(next.node)
			v.Revision = 2
			s.set(v)
			check(t, c.refresh(context.Background()))
			var out int
			check(t, c.Call(context.Background(), "/test/echo", 1, &out))
			if out != 21 || c.Stats().Retiring != 1 || c.Stats().Connections != 2 || buffered(caller) == 0 {
				t.Fatal(out, c.Stats())
			}
			for i := 0; i < 3; i++ {
				check(t, stream.Recv(&out))
				if out != 10+i {
					t.Fatal(out)
				}
			}
			if err := stream.Recv(nil); err != io.EOF {
				t.Fatal(err)
			}
			until(t, func() bool { return c.Stats().Connections == 1 && old.s.Stats().Connections == 0 })
			if buffered(caller) != 0 {
				t.Fatal("retired buffers remain")
			}
			canceled, cancel := context.WithCancel(context.Background())
			active, err := c.Stream(canceled, "/test/cancel", 1)
			check(t, err)
			defer active.Close()
			check(t, active.Recv(&out))
			cancel()
			if err := active.Recv(&out); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
			receive(t, next.canceled)
			c.Close()
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			check(t, c.Wait(ctx))
			if c.Stats().ActiveCalls != 0 {
				t.Fatal(c.Stats())
			}
		})
	}
}

type blockingDecoder struct {
	entered chan struct{}
	release <-chan struct{}
}

func (b blockingDecoder) UnmarshalJSON([]byte) error { close(b.entered); <-b.release; return nil }

type panicEncoder struct{}

func (panicEncoder) MarshalJSON() ([]byte, error) { panic("fixture user marshal") }

type panicDecoder struct{}

func (panicDecoder) UnmarshalJSON([]byte) error { panic("fixture user unmarshal") }

func TestCloseWaitKeepsBlockedDecodeAndUnreadOwnershipVisible(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	f := serve(t, rpc.TCP, credentials, 10)
	c := managed(t, &source{snapshot: fresh(f.node)}, Config{Dial: options(credentials)}, rpc.DialEndpoint)
	stream, err := c.Stream(context.Background(), "/test/items", 3)
	check(t, err)
	defer stream.Close()
	release, entered := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	done := make(chan error, 1)
	go func() { done <- stream.Recv(&blockingDecoder{entered, release}) }()
	receive(t, entered)
	check(t, stream.Close())
	c.Close()
	short, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	if err := c.Wait(short); !errors.Is(err, context.DeadlineExceeded) || c.Stats().ActiveCalls != 1 {
		t.Fatal("blocked decoder disappeared", err, c.Stats())
	}
	once.Do(func() { close(release) })
	check(t, receive(t, done))
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	check(t, c.Wait(ctx))
}
func TestForgottenStreamAndUserCodecPanics(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	f := serve(t, rpc.TCP, credentials, 10)
	c := managed(t, &source{snapshot: fresh(f.node)}, Config{Dial: options(credentials), MaxInFlight: 2}, rpc.DialEndpoint)
	for _, streamCall := range []bool{false, true} {
		func() {
			defer func() {
				if recover() == nil {
					t.Error("marshal panic swallowed")
				}
			}()
			if streamCall {
				_, _ = c.Stream(context.Background(), "/test/items", panicEncoder{})
			} else {
				_ = c.Call(context.Background(), "/test/echo", panicEncoder{}, new(int))
			}
		}()
		if c.Stats().ActiveCalls != 0 {
			t.Fatal("marshal panic leaked", c.Stats())
		}
	}
	stream, err := c.Stream(context.Background(), "/test/items", 3)
	check(t, err)
	func() {
		defer func() {
			if recover() == nil {
				t.Error("decoder panic swallowed")
			}
		}()
		_ = stream.Recv(&panicDecoder{})
	}()
	if c.Stats().ActiveCalls != 0 {
		t.Fatal("decode panic leaked", c.Stats())
	}
	stream, err = c.Stream(context.Background(), "/test/items", 3)
	check(t, err)
	c.Close()
	short, stop := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer stop()
	if err := c.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("forgotten stream disappeared", err)
	}
	check(t, stream.Close())
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	check(t, c.Wait(ctx))
}

func TestTransportFailureDoesNotReplayExecutedBusiness(t *testing.T) {
	for _, kind := range []rpc.Transport{rpc.TCP, rpc.TLS, rpc.QUIC} {
		t.Run(string(kind), func(t *testing.T) {
			credentials, err := testcert.New()
			check(t, err)
			old := serve(t, kind, credentials, 10)
			next := serve(t, kind, credentials, 20)
			s := &source{snapshot: fresh(old.node)}
			c := managed(t, s, Config{Transport: kind, Dial: options(credentials), MaxConnections: 2}, rpc.DialEndpoint)
			done := make(chan error, 1)
			go func() { done <- c.Call(context.Background(), "/test/hold", 1, new(int)) }()
			receive(t, old.started)
			v := fresh(next.node)
			v.Revision = 2
			s.set(v)
			check(t, c.refresh(context.Background()))
			old.s.Close()
			if err := receive(t, done); err == nil {
				t.Fatal("lost call appeared successful")
			}
			if old.executions.Load() != 1 || next.executions.Load() != 0 {
				t.Fatal("executed call replayed", old.executions.Load(), next.executions.Load())
			}
			var out int
			check(t, c.Call(context.Background(), "/test/echo", 1, &out))
			if out != 21 || next.executions.Load() != 1 {
				t.Fatal(out, next.executions.Load())
			}
		})
	}
}

func TestRealEtcdEndpointUpdateDeletionAndResolverOwnership(t *testing.T) {
	address := os.Getenv("KANARPC_ETCD_ENDPOINT")
	if address == "" {
		t.Skip("owned etcd required: scripts/verify-discovery.py")
	}
	for _, kind := range []rpc.Transport{rpc.TCP, rpc.TLS, rpc.QUIC} {
		t.Run(string(kind), func(t *testing.T) {
			credentials, err := testcert.New()
			check(t, err)
			old := serve(t, kind, credentials, 10)
			next := serve(t, kind, credentials, 20)
			namespace := fmt.Sprintf("/kanarpc/r8c/%s-%d-%d", strings.ReplaceAll(t.Name(), "/", "-"), os.Getpid(), time.Now().UnixNano())
			r, err := discovery.New(context.Background(), discovery.Config{Endpoints: []string{address}, Namespace: namespace, RequestTimeout: time.Second, FreshnessTTL: 2 * time.Second})
			check(t, err)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				check(t, r.Close(ctx))
			})
			registration, err := r.Register(context.Background(), "service", old.node, 2)
			check(t, err)
			resolver, err := r.Watch(context.Background(), "service", kind)
			check(t, err)
			until(t, func() bool { return registration.State().Ready && len(resolver.Snapshot().Endpoints) == 1 })
			c := managed(t, resolver, Config{Transport: kind, Dial: options(credentials), MaxConnections: 2}, rpc.DialEndpoint)
			stream, err := c.Stream(context.Background(), "/test/items", 3)
			check(t, err)
			defer stream.Close()
			caller := callerFor(t, c, old.node)
			until(t, func() bool {
				return old.s.Stats().Delivered == 1 && old.s.Stats().InFlight == 0 && buffered(caller) > 0
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			check(t, registration.Close(ctx))
			registration, err = r.Register(context.Background(), "service", next.node, 2)
			check(t, err)
			until(t, func() bool {
				v := resolver.Snapshot()
				return v.Ready && len(v.Endpoints) == 1 && v.Endpoints[0] == next.node
			})
			var out int
			check(t, c.Call(context.Background(), "/test/echo", 1, &out))
			if out != 21 || c.Stats().Retiring != 1 {
				t.Fatal(out, c.Stats())
			}
			for i := 0; i < 3; i++ {
				check(t, stream.Recv(&out))
				if out != 10+i {
					t.Fatal(out)
				}
			}
			if err := stream.Recv(nil); err != io.EOF {
				t.Fatal(err)
			}
			until(t, func() bool { return old.s.Stats().Connections == 0 && c.Stats().Connections == 1 })
			check(t, registration.Close(ctx))
			until(t, func() bool { return resolver.Snapshot().Ready && len(resolver.Snapshot().Endpoints) == 0 })
			if err := invoke(c, context.Background()); !errors.Is(err, ErrNotReady) {
				t.Fatal(err)
			}
			until(t, func() bool { return c.Stats().Connections == 0 })
			c.Close()
			check(t, c.Wait(ctx))
			if r.Stats().Resolvers != 1 {
				t.Fatal("manager closed caller-owned resolver", r.Stats())
			}
			check(t, resolver.Close(ctx))
		})
	}
}

func FuzzSnapshotFence(f *testing.F) {
	f.Add([]byte(`{"Ready":true,"Revision":1,"ClusterID":1,"Endpoints":[{"id":"a","address":"127.0.0.1:19091","transport":"tcp"}]}`))
	f.Add([]byte(`{"Ready":true,"Revision":0}`))
	f.Add([]byte(`{"Ready":false}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 8192 {
			return
		}
		var v discovery.Snapshot
		if json.Unmarshal(data, &v) != nil {
			return
		}
		s := &source{snapshot: v}
		var dials atomic.Int64
		c := managed(t, s, Config{MaxEndpoints: 4}, func(context.Context, rpc.Endpoint, rpc.DialOptions) (rpc.Caller, error) {
			dials.Add(1)
			return &fakeCaller{}, nil
		})
		err := invoke(c, context.Background())
		stats := c.Stats()
		if stats.Connections > 8 || stats.PeakConnections > 8 || stats.ActiveCalls != 0 {
			t.Fatal(stats)
		}
		if err == nil && (!v.Ready || v.Revision < 1 || v.ClusterID == 0 || len(v.Endpoints) == 0 || len(v.Endpoints) > 4 || dials.Load() != 1) {
			t.Fatal(v, stats)
		}
	})
}
