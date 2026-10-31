package routing

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/netfault"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

func TestPoolRetiredUnreadAndDecoderOwnership(t *testing.T) {
	for _, kind := range []rpc.Transport{rpc.TCP, rpc.TLS, rpc.QUIC} {
		t.Run(string(kind), func(t *testing.T) {
			credentials, err := testcert.New()
			check(t, err)
			old, next := serve(t, kind, credentials, 10), serve(t, kind, credentials, 20)
			source := &source{snapshot: fresh(old.node)}
			c := managed(t, source, Config{Transport: kind, Dial: options(credentials), MaxConnections: 2}, rpc.DialEndpoint)
			stream, err := c.Stream(context.Background(), "/test/items", 3)
			check(t, err)
			defer stream.Close()
			until(t, func() bool { return old.s.Stats().InFlight == 0 && old.s.Stats().Delivered == 1 })
			before := c.MemoryStats()
			if before.Unread.Streams != 1 || before.Unread.Chunks != 3 || before.Unread.Bytes != 6 || before.Memory.Receive.Used != 6 {
				t.Fatal(before)
			}
			snapshot := fresh(next.node)
			snapshot.Revision = 2
			source.set(snapshot)
			check(t, c.refresh(context.Background()))
			check(t, c.Call(context.Background(), "/test/echo", 1, new(int)))
			retired := c.MemoryStats()
			if retired.Retired.Accounts != 1 || retired.Retired.DataBytes != 6 || retired.Retired.Unread.Bytes != 6 || retired.Accounts != 2 {
				t.Fatal(retired)
			}
			views := c.ConnectionStats()
			if len(views) != 2 || !views[0].Retiring || views[0].Memory.Unread.Chunks != 3 || views[0].Users != 1 || views[0].RetiredFor <= 0 {
				t.Fatal(views)
			}
			release, entered := make(chan struct{}), make(chan struct{})
			var once sync.Once
			defer once.Do(func() { close(release) })
			done := make(chan error, 1)
			go func() { done <- stream.Recv(&blockingDecoder{entered, release}) }()
			receive(t, entered)
			held := c.MemoryStats()
			if held.Retired.Unread.Chunks != 2 || held.Memory.Receive.Used != 6 {
				t.Fatal("decoder lease lost", held)
			}
			check(t, stream.Close())
			c.Close()
			until(t, func() bool {
				views := c.ConnectionStats()
				for _, v := range views {
					if v.Memory.ID == 1 {
						return v.Closed && v.Memory.OwnerClosed
					}
				}
				return false
			})
			held = c.MemoryStats()
			if held.Retired.Unread.Bytes != 0 || held.Memory.Receive.Used != 2 || held.Retired.Leases != 1 {
				t.Fatal("closed decoder uncharged", held)
			}
			short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()
			if err := c.Wait(short); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			once.Do(func() { close(release) })
			check(t, receive(t, done))
			ctx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			check(t, c.Wait(ctx))
			final := c.MemoryStats()
			if final.Accounts != 0 || final.Total.Used != 0 || final.Released != final.Created || final.Unread.Chunks != 0 || !final.Sealed {
				t.Fatal(final)
			}
		})
	}
}
func TestPoolRetiredStreamConsumptionAndAccountChurn(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	old, next := serve(t, rpc.QUIC, credentials, 10), serve(t, rpc.QUIC, credentials, 20)
	source := &source{snapshot: fresh(old.node)}
	c := managed(t, source, Config{Transport: rpc.QUIC, Dial: options(credentials), MaxConnections: 2}, rpc.DialEndpoint)
	for i := int64(1); i <= 40; i++ {
		stream, err := c.Stream(context.Background(), "/test/items", 3)
		check(t, err)
		until(t, func() bool { return c.MemoryStats().Unread.Chunks == 3 })
		node := next.node
		if i%2 == 0 {
			node = old.node
		}
		snapshot := fresh(node)
		snapshot.Revision = i + 1
		source.set(snapshot)
		check(t, c.refresh(context.Background()))
		for n := 0; n < 3; n++ {
			check(t, stream.Recv(new(int)))
		}
		if e := stream.Recv(nil); e != io.EOF {
			t.Fatal(e)
		}
		until(t, func() bool { return c.Stats().Connections == 0 })
		s := c.MemoryStats()
		if s.Accounts != 0 || s.Total.Used != 0 || s.Created != uint64(i) || s.Released != uint64(i) || s.PeakAccounts > 2 {
			t.Fatal(s)
		}
	}
}
func TestPoolBlockedEncoderCloseRemainsCharged(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	f := serve(t, rpc.TCP, credentials, 10)
	cfg := Config{Dial: options(credentials)}
	cfg.Dial.Client.IO.MaxFrameBytes = 1024
	testPoolMemory(&cfg, 2048, 4096, 4096, 16384)
	c := managed(t, &source{snapshot: fresh(f.node)}, cfg, rpc.DialEndpoint)
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var encoder poolEncoder
	encoder.entered = entered
	encoder.release = release
	encoder.count = new(atomic.Int64)
	done := make(chan error, 1)
	go func() { done <- c.Call(context.Background(), "/test/echo", encoder, new(int)) }()
	receive(t, entered)
	c.Close()
	until(t, func() bool { return c.MemoryStats().Retired.Accounts == 1 })
	s := c.MemoryStats()
	if s.Retired.Bytes.Encoding != 2048 || s.Retired.Leases != 1 {
		t.Fatal(s)
	}
	short, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if c.Wait(short) == nil {
		t.Fatal("blocked codec lost")
	}
	once.Do(func() { close(release) })
	if receive(t, done) == nil {
		t.Fatal("closed preparation sent")
	}
	ctx, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	check(t, c.Wait(ctx))
	if c.MemoryStats().Accounts != 0 || f.executions.Load() != 0 {
		t.Fatal(c.MemoryStats())
	}
}

func TestPoolLateDialAccountSurvivesSealUntilOwnedClose(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	f := serve(t, rpc.TCP, credentials, 10)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	c := managed(t, &source{snapshot: fresh(f.node)}, Config{Dial: options(credentials), MaxConnections: 1}, func(_ context.Context, node rpc.Endpoint, options rpc.DialOptions) (rpc.Caller, error) {
		close(entered)
		<-release
		return rpc.DialEndpoint(context.Background(), node, options)
	})
	done := make(chan error, 1)
	go func() { done <- c.Call(context.Background(), "/test/echo", 1, new(int)) }()
	receive(t, entered)
	c.Close()
	if receive(t, done) == nil {
		t.Fatal("closed call succeeded")
	}
	s := c.MemoryStats()
	views := c.ConnectionStats()
	if s.Retired.Accounts != 1 || !s.Sealed || len(views) != 1 || views[0].Memory.Bound || !views[0].Dialing {
		t.Fatal(s, views)
	}
	once.Do(func() { close(release) })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	check(t, c.Wait(ctx))
	s = c.MemoryStats()
	if s.Created != 1 || s.Released != 1 || s.Total.Used != 0 || s.Accounts != 0 || f.executions.Load() != 0 {
		t.Fatal(s)
	}
}

func TestPoolSnapshotsUnderConcurrentCloseAndChurn(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	f := serve(t, rpc.QUIC, credentials, 10)
	c := managed(t, &source{snapshot: fresh(f.node)}, Config{Transport: rpc.QUIC, Dial: options(credentials), MaxInFlight: 32}, rpc.DialEndpoint)
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				s := c.MemoryStats()
				_ = c.ConnectionStats()
				if s.Data.Used < 0 || s.Data.Used > s.Data.Limit || s.Total.Peak > s.Total.Limit || s.Accounts > s.Limit {
					t.Error(s)
					return
				}
			}
		}()
	}
	for i := 0; i < 100; i++ {
		check(t, c.Call(context.Background(), "/test/echo", 1, new(int)))
	}
	c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	check(t, c.Wait(ctx))
	close(stop)
	wg.Wait()
	if c.MemoryStats().Accounts != 0 {
		t.Fatal(c.MemoryStats())
	}
}

func TestPoolReceiveSaturationPreservesOtherEndpointAllTransports(t *testing.T) {
	for _, kind := range []rpc.Transport{rpc.TCP, rpc.TLS, rpc.QUIC} {
		t.Run(string(kind), func(t *testing.T) {
			credentials, err := testcert.New()
			check(t, err)
			a, b := serve(t, kind, credentials, 1<<60), serve(t, kind, credentials, 1<<60)
			a.node.ID = "a"
			b.node.ID = "b"
			cfg := Config{Transport: kind, Dial: options(credentials), MaxConnections: 2}
			cfg.Dial.Client.StreamWindow = 32
			cfg.Dial.Client.IO.MaxFrameBytes = 1024
			testPoolMemory(&cfg, 4096, 1024, 4096, 16384)
			source := &source{snapshot: fresh(a.node, b.node)}
			c := managed(t, source, cfg, rpc.DialEndpoint)
			one, err := c.Stream(context.Background(), "/test/items", 32)
			check(t, err)
			defer one.Close()
			until(t, func() bool { return a.s.Stats().Delivered == 1 && a.s.Stats().InFlight == 0 })
			two, err := c.Stream(context.Background(), "/test/items", 32)
			check(t, err)
			defer two.Close()
			until(t, func() bool { return b.s.Stats().InFlight == 0 && c.MemoryStats().Memory.Receive.Rejected > 0 })
			before := c.ConnectionStats()
			var bID uint64
			for _, v := range before {
				if v.Endpoint == b.node {
					bID = v.Memory.ID
				}
			}
			if bID == 0 || c.MemoryStats().Memory.Receive.Peak > 1024 {
				t.Fatal(before, c.MemoryStats())
			}
			for i := 0; i < 32; i++ {
				check(t, one.Recv(new(int)))
			}
			if e := one.Recv(nil); e != io.EOF {
				t.Fatal(e)
			}
			if e := two.Recv(nil); e == nil || e == io.EOF {
				t.Fatal("saturation hidden", e)
			}
			two.Close()
			snapshot := fresh(b.node)
			snapshot.Revision = 2
			source.set(snapshot)
			check(t, c.refresh(context.Background()))
			check(t, c.Call(context.Background(), "/test/echo", 1, new(int)))
			var afterID uint64
			for _, v := range c.ConnectionStats() {
				if v.Endpoint == b.node {
					afterID = v.Memory.ID
				}
			}
			if kind == rpc.QUIC && afterID != bID {
				t.Fatal("native stream failure retired whole session", bID, afterID)
			}
			if kind != rpc.QUIC && afterID == bID {
				t.Fatal("byte-stream framing failure reused damaged connection", bID, afterID)
			}
		})
	}
}

func TestPoolQUICBlackholeCancellationRetirementAndClose(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	f := serve(t, rpc.QUIC, credentials, 10)
	relay, err := netfault.NewUDP(f.node.Address)
	check(t, err)
	defer relay.Close()
	defer relay.Set(netfault.ToClient, netfault.Plan{})
	node := f.node
	node.Address = relay.Address()
	source := &source{snapshot: fresh(node)}
	cfg := Config{Transport: rpc.QUIC, Dial: options(credentials)}
	cfg.Dial.Client.ControlTimeout = 40 * time.Millisecond
	c := managed(t, source, cfg, rpc.DialEndpoint)
	stream, err := c.Stream(context.Background(), "/test/items", 3)
	check(t, err)
	defer stream.Close()
	until(t, func() bool { return c.MemoryStats().Unread.Chunks == 3 && f.s.Stats().InFlight == 0 })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Call(ctx, "/test/hold", 1, new(int)) }()
	receive(t, f.started)
	check(t, relay.Set(netfault.ToClient, netfault.Plan{Blackhole: true}))
	cancel()
	if e := receive(t, done); !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	receive(t, f.canceled)
	snapshot := fresh()
	snapshot.Revision = 2
	source.set(snapshot)
	check(t, c.refresh(context.Background()))
	if s := c.MemoryStats(); s.Retired.Unread.Bytes != 6 || s.Retired.Accounts != 1 {
		t.Fatal(s)
	}
	begin := time.Now()
	c.Close()
	check(t, stream.Close())
	wait, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	check(t, c.Wait(wait))
	if time.Since(begin) > time.Second {
		t.Fatal("blackhole shutdown unbounded")
	}
	if s := c.MemoryStats(); s.Total.Used != 0 || s.Accounts != 0 || s.Released != s.Created {
		t.Fatal(s)
	}
	until(t, func() bool { return relay.Stats().ToClient.Dropped > 0 })
	check(t, relay.Set(netfault.ToClient, netfault.Plan{}))
	t.Logf("local UDP reverse-blackhole; relay=%+v final_pool=%+v", relay.Stats(), c.MemoryStats())
}
