package kanarpc

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

func TestNetworkCancelAndDeadlineDrainPhysicalTransports(t *testing.T) {
	kernel := os.Getenv("KANARPC_KERNEL_BLACKHOLE") == "1"
	// Never modify a host qdisc. The opt-in runner executes this test as PID 1
	// in a dedicated --network none container with only its lo interface.
	if kernel && (runtime.GOOS != "linux" || os.Getpid() != 1 || os.Getenv("KANARPC_OWNED_NETNS") != "1") {
		t.Fatal("kernel injection requires the owned isolated container runner")
	}
	tc := func(args ...string) string {
		t.Helper()
		output, err := exec.Command("tc", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("tc %v: %v: %s", args, err, output)
		}
		return string(output)
	}
	for _, kind := range []Transport{TCP, TLS, QUIC} {
		t.Run(string(kind), func(t *testing.T) {
			credentials, err := testcert.New()
			require(t, err)
			s, err := NewServer(ServerConfig{})
			require(t, err)
			started, canceled := make(chan struct{}), make(chan struct{})
			var executions atomic.Int64
			require(t, RegisterUnary(s, "/network/hold", func(ctx context.Context, n int) (int, error) {
				executions.Add(1)
				close(started)
				<-ctx.Done()
				close(canceled)
				return n, ctx.Err()
			}))
			done := make(chan error, 1)
			var address string
			if kind == QUIC {
				l, e := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{})
				require(t, e)
				address = l.Addr().String()
				go func() { done <- s.ServeQUIC(l) }()
			} else {
				l, e := net.Listen("tcp", "127.0.0.1:0")
				require(t, e)
				address = l.Addr().String()
				go func() {
					if kind == TLS {
						done <- s.ServeTLS(l, TLSOptions{Config: credentials.Server})
					} else {
						done <- s.Serve(l)
					}
				}()
			}
			eventually(t, s.Ready)
			var c Caller
			defer func() {
				if kernel {
					_ = exec.Command("tc", "qdisc", "del", "dev", "lo", "root").Run()
				}
				if c != nil {
					c.Close()
				}
				s.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				require(t, s.Wait(ctx))
				require(t, <-done)
			}()
			c, err = DialEndpoint(context.Background(), Endpoint{Address: address, Transport: kind}, DialOptions{TLS: TLSOptions{Config: credentials.Client}, Client: ClientConfig{Timeout: 3 * time.Second, ControlTimeout: 300 * time.Millisecond}})
			require(t, err)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { var out int; result <- c.Call(ctx, "/network/hold", 1, &out) }()
			signal(t, started)
			if kernel {
				tc("qdisc", "replace", "dev", "lo", "root", "netem", "loss", "100%")
			}
			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatal("user cancellation identity lost", err)
			}
			if !kernel {
				signal(t, canceled)
			} else {
				// Give the async cancel writer time to emit a real packet into netem.
				time.Sleep(15 * time.Millisecond)
				select {
				case <-canceled:
					t.Fatal("100% blackhole did not block remote cancel")
				default:
				}
				if s.Stats().Executing != 1 {
					t.Fatal("handler ended before forced drain", s.Stats())
				}
			}
			drain, stop := context.WithTimeout(context.Background(), 40*time.Millisecond)
			defer stop()
			begin := time.Now()
			err = s.Drain(drain)
			elapsed := time.Since(begin)
			if kernel {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal("drain failed to expose undelivered cancellation", err)
				}
				t.Log(tc("-s", "qdisc", "show", "dev", "lo"))
				tc("qdisc", "del", "dev", "lo", "root")
			} else {
				require(t, err)
			}
			if elapsed > time.Second {
				t.Fatal("deadline drain exceeded bound", elapsed)
			}
			signal(t, canceled)
			c.Close()
			wait, stopWait := context.WithTimeout(context.Background(), time.Second)
			defer stopWait()
			require(t, s.Wait(wait))
			if idle, ok := c.(interface{ WaitIdle(context.Context) error }); ok {
				require(t, idle.WaitIdle(wait))
			}
			if executions.Load() != 1 || s.Stats().Admitted != 1 {
				t.Fatal("unexpected replay/admission", executions.Load(), s.Stats())
			}
			faultLog(t, map[string]any{"transport": kind, "kernel_blackhole": kernel, "drain_ns": elapsed.Nanoseconds(), "executions": executions.Load(), "server": s.Stats(), "scope": "owned physical loopback; kernel blackhole only when explicitly running isolated Linux container"})
		})
	}
}
