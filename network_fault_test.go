package kanarpc

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/netfault"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

type faultFixture struct {
	s                 *Server
	c                 *QUICClient
	relay             *netfault.UDP
	calls             atomic.Int64
	started, canceled chan struct{}
}

func faultQUIC(t testing.TB) *faultFixture {
	t.Helper()
	credentials, err := testcert.New()
	require(t, err)
	s, err := NewServer(ServerConfig{AckTimeout: 2 * time.Second})
	require(t, err)
	f := &faultFixture{s: s, started: make(chan struct{}, 8), canceled: make(chan struct{}, 8)}
	require(t, RegisterUnary(s, "/fault/echo", func(_ context.Context, in string) (string, error) { f.calls.Add(1); return in, nil }))
	require(t, RegisterUnary(s, "/fault/hold", func(ctx context.Context, in string) (string, error) {
		f.calls.Add(1)
		f.started <- struct{}{}
		<-ctx.Done()
		f.canceled <- struct{}{}
		return in, ctx.Err()
	}))
	require(t, RegisterStream(s, "/fault/items", func(ctx context.Context, n int, send func(int) error) error {
		f.calls.Add(1)
		for i := 0; i < n; i++ {
			if err := send(i); err != nil {
				return err
			}
		}
		return ctx.Err()
	}))
	l, err := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{})
	require(t, err)
	done := make(chan error, 1)
	go func() { done <- s.ServeQUIC(l) }()
	eventually(t, s.Ready)
	relay, err := netfault.NewUDP(l.Addr().String())
	require(t, err)
	f.relay = relay
	c, err := DialQUIC(context.Background(), relay.Address(), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{Timeout: 3 * time.Second, ControlTimeout: 300 * time.Millisecond, StreamWindow: 2})
	require(t, err)
	f.c = c
	t.Cleanup(func() {
		_ = relay.Set(netfault.ToServer, netfault.Plan{})
		_ = relay.Set(netfault.ToClient, netfault.Plan{})
		c.Close()
		s.Close()
		relay.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require(t, c.WaitIdle(ctx))
		require(t, s.Wait(ctx))
		require(t, <-done)
		if relay.Stats().ToServer.PendingBytes != 0 || relay.Stats().ToClient.PendingBytes != 0 {
			t.Error("relay retained packet")
		}
	})
	return f
}
func faultLog(t testing.TB, value any) {
	t.Helper()
	b, err := json.Marshal(value)
	require(t, err)
	t.Log(string(b))
}

func TestQUICInjectedPathBaselineDelayLossAndRate(t *testing.T) {
	for _, profile := range []string{"baseline", "delay", "loss", "rate"} {
		t.Run(profile, func(t *testing.T) {
			f := faultQUIC(t)
			var out string
			require(t, f.c.Call(context.Background(), "/fault/echo", "warmup", &out))
			plan := netfault.Plan{}
			payload := "payload"
			switch profile {
			case "delay":
				plan.Delay = 2 * time.Millisecond
			case "loss":
				plan.DropNext = 1
			case "rate":
				plan.BytesPerSecond = 32 << 10
				payload = strings.Repeat("x", 4096)
			}
			require(t, f.relay.Set(netfault.ToServer, plan))
			require(t, f.relay.Set(netfault.ToClient, plan))
			begin := time.Now()
			require(t, f.c.Call(context.Background(), "/fault/echo", payload, &out))
			elapsed := time.Since(begin)
			if out != payload || f.calls.Load() != 2 {
				t.Fatal("corruption or replay", out, f.calls.Load())
			}
			if profile == "delay" && elapsed < 4*time.Millisecond {
				t.Fatal("delay not applied", elapsed)
			}
			if profile == "rate" && elapsed < 100*time.Millisecond {
				t.Fatal("rate plan not applied", elapsed)
			}
			eventually(t, func() bool {
				return f.s.Stats().InFlight == 0 && f.s.Stats().NativeStreams == 0 && f.c.AdmissionStats().Active == 0
			})
			stats := f.relay.Stats()
			if profile == "loss" && (stats.ToServer.Dropped == 0 || stats.ToClient.Dropped == 0) {
				t.Fatal("loss was not observed", stats)
			}
			if stats.ToServer.PeakPendingBytes > 65536 || stats.ToClient.PeakPendingBytes > 65536 {
				t.Fatal(stats)
			}
			faultLog(t, map[string]any{"profile": profile, "elapsed_ns": elapsed.Nanoseconds(), "plan": plan, "relay": stats, "server": f.s.Stats(), "evidence": "real loopback UDP application relay; not kernel netem, dual host or inference performance"})
		})
	}
}

func TestQUICReverseBlackholeCancellationAndRecovery(t *testing.T) {
	f := faultQUIC(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { var out string; done <- f.c.Call(ctx, "/fault/hold", "once", &out) }()
	signal(t, f.started)
	require(t, f.relay.Set(netfault.ToClient, netfault.Plan{Blackhole: true}))
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	signal(t, f.canceled)
	eventually(t, func() bool { return f.relay.Stats().ToClient.Dropped > 0 })
	require(t, f.relay.Set(netfault.ToClient, netfault.Plan{}))
	var out string
	require(t, f.c.Call(context.Background(), "/fault/echo", "healthy", &out))
	if out != "healthy" || f.calls.Load() != 2 {
		t.Fatal("replayed canceled business", f.calls.Load(), out)
	}
	eventually(t, func() bool {
		return f.c.AdmissionStats().Active == 0 && f.s.Stats().InFlight == 0 && f.s.Stats().NativeStreams == 0
	})
	faultLog(t, map[string]any{"case": "cancel_reverse_blackhole_restore", "relay": f.relay.Stats(), "client": f.c.Stats(), "server": f.s.Stats(), "executions": f.calls.Load()})
}

func TestQUICSlowConsumerAndDeadlineDrainUnderBlackhole(t *testing.T) {
	t.Run("slow_consumer_neighbor_and_cancel", func(t *testing.T) {
		f := faultQUIC(t)
		stream, err := f.c.Stream(context.Background(), "/fault/items", 20)
		require(t, err)
		defer stream.Close()
		eventually(t, func() bool { return f.c.MemoryStats().Receive.Used > 0 && f.s.Stats().Executing == 1 })
		var out string
		require(t, f.c.Call(context.Background(), "/fault/echo", "neighbor", &out))
		if out != "neighbor" {
			t.Fatal(out)
		}
		require(t, stream.Close())
		eventually(t, func() bool {
			return f.s.Stats().InFlight == 0 && f.s.Stats().NativeStreams == 0 && f.c.MemoryStats().Receive.Used == 0
		})
		if f.calls.Load() != 2 {
			t.Fatal(f.calls.Load())
		}
		faultLog(t, map[string]any{"case": "slow_consumer_cancel", "client": f.c.Stats(), "server": f.s.Stats(), "memory": f.c.MemoryStats()})
	})
	t.Run("blackhole_deadline_drain", func(t *testing.T) {
		f := faultQUIC(t)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { var out string; done <- f.c.Call(ctx, "/fault/hold", "once", &out) }()
		signal(t, f.started)
		require(t, f.relay.Set(netfault.ToServer, netfault.Plan{Blackhole: true}))
		require(t, f.relay.Set(netfault.ToClient, netfault.Plan{Blackhole: true}))
		drain, stop := context.WithTimeout(context.Background(), 40*time.Millisecond)
		defer stop()
		start := time.Now()
		err := f.s.Drain(drain)
		elapsed := time.Since(start)
		if !errors.Is(err, context.DeadlineExceeded) || elapsed > time.Second {
			t.Fatal("drain budget escaped", err, elapsed)
		}
		signal(t, f.canceled)
		cancel()
		if err = <-done; err == nil {
			t.Fatal("lost request appeared successful")
		}
		require(t, f.relay.Set(netfault.ToServer, netfault.Plan{}))
		require(t, f.relay.Set(netfault.ToClient, netfault.Plan{}))
		f.c.Close()
		wait, stopWait := context.WithTimeout(context.Background(), time.Second)
		defer stopWait()
		require(t, f.s.Wait(wait))
		require(t, f.c.WaitIdle(wait))
		if f.calls.Load() != 1 || f.s.Stats().Admitted != 1 {
			t.Fatal("business replay or post-drain admission", f.calls.Load(), f.s.Stats())
		}
		faultLog(t, map[string]any{"case": "blackhole_deadline_drain", "elapsed_ns": elapsed.Nanoseconds(), "relay": f.relay.Stats(), "server": f.s.Stats(), "client_admission": f.c.AdmissionStats()})
	})
}
