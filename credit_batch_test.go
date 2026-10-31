package kanarpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func creditFixture(t *testing.T, transport string, window uint32, register func(*Server)) (*Server, Caller) {
	t.Helper()
	cfg := ClientConfig{StreamWindow: int(window)}
	if transport == "quic" {
		s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, register, cfg)
		return s, c
	}
	s, address := start(t, ServerConfig{}, register)
	return s, connect(t, address, cfg)
}

// More than three windows force repeated replenishment, including an odd
// window and a final incomplete batch. Completion must still settle its ACK.
func TestCreditBatchWindowsProgressAndDrain(t *testing.T) {
	for _, transport := range []string{"tcp", "quic"} {
		for _, window := range []uint32{1, 2, 3, 4, 7, 31, 32} {
			t.Run(fmt.Sprintf("%s/%d", transport, window), func(t *testing.T) {
				count := int(window)*3 + 3
				s, c := creditFixture(t, transport, window, func(s *Server) {
					require(t, RegisterStream(s, "/batch/items", func(ctx context.Context, n int, send func(int) error) error {
						for i := 0; i < n; i++ {
							if err := send(i); err != nil {
								return err
							}
						}
						return nil
					}))
				})
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				stream, err := c.Stream(ctx, "/batch/items", count)
				require(t, err)
				defer stream.Close()
				for i := 0; i < count; i++ {
					var out int
					require(t, stream.Recv(&out))
					if out != i {
						t.Fatal(i, out)
					}
				}
				if err = stream.Recv(nil); err != io.EOF {
					t.Fatal(err)
				}
				report, err := s.DrainWithReport(ctx)
				require(t, err)
				if !report.Quiescent || report.TotalFaults.LostTerminals != 0 || report.TotalFaults.IncompleteExchanges != 0 || s.Stats().Delivered != 1 {
					t.Fatal(report, s.Stats())
				}
			})
		}
	}
}

func TestCreditBatchWithheldCreditsCancel(t *testing.T) {
	for _, transport := range []string{"tcp", "quic"} {
		t.Run(transport, func(t *testing.T) {
			stopped := make(chan struct{})
			s, c := creditFixture(t, transport, 32, func(s *Server) {
				require(t, RegisterStream(s, "/batch/forever", func(ctx context.Context, _ int, send func(int) error) error {
					defer close(stopped)
					for i := 0; ; i++ {
						if err := send(i); err != nil {
							return err
						}
					}
				}))
			})
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			stream, err := c.Stream(ctx, "/batch/forever", 0)
			require(t, err)
			for i := 0; i < 15; i++ {
				var out int
				require(t, stream.Recv(&out))
			}
			require(t, stream.Close())
			signal(t, stopped)
			eventually(t, func() bool {
				return s.Stats().InFlight == 0 && c.(interface{ MemoryStats() MemoryStats }).MemoryStats().Receive.Used == 0
			})
			if err = stream.Recv(nil); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestCreditBatchPoolReservationCancellationAndRefusal(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	pool, rs, _ := poolFixture(t, 1)
	cfg, err := normalizeIO(IOConfig{})
	require(t, err)
	link := newLink(a, cfg)
	link.resources = rs[0]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := link.sendCredits(ctx, 1, 16); done <- err }()
	eventually(t, func() bool { return pool.Stats().Memory.ControlSend.Used == 16*24 })
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("credit write ignored cancellation")
	}
	if stats := pool.Stats(); stats.Total.Used != 0 || stats.Active.Leases != 0 {
		t.Fatal(stats)
	}
	// One byte less than a complete packet must refuse before touching the pipe.
	held, err := rs[0].controlSend.take(rs[0].controlSend.stats.Limit - 16*24 + 1)
	require(t, err)
	touched, err := link.sendCredits(context.Background(), 1, 16)
	var budget *BudgetError
	if touched || !errors.As(err, &budget) || budget.Class != "control_send" {
		t.Fatal(touched, err)
	}
	held()
	if stats := pool.Stats(); stats.Total.Used != 0 || stats.Active.Leases != 0 {
		t.Fatal(stats)
	}
}
