package kanarpc

import (
	"context"
	"io"
	"testing"
	"time"
	"unsafe"
)

// Local accounting microbenchmarks deliberately exclude IO, encoding and QUIC.
// They isolate synchronization cost; they are not RPC capacity measurements.
func BenchmarkStopObservation(b *testing.B) {
	for _, mode := range []string{"acked_live", "acked_fin", "acked_deadline", "canceled", "transport"} {
		b.Run(mode, func(b *testing.B) {
			s := &Server{}
			b.ReportAllocs()
			b.ReportMetric(float64(unsafe.Sizeof(execution{})), "execution-bytes")
			b.RunParallel(func(pb *testing.PB) {
				parent, parentCancel := context.WithCancelCause(context.Background())
				defer parentCancel(nil)
				var ctx context.Context
				var stop context.CancelFunc
				if mode == "acked_deadline" {
					ctx, stop = context.WithDeadline(parent, time.Unix(1, 0))
					defer stop()
				} else {
					ctx = parent
				}
				r := &execution{peer: &serverConn{server: s, ctx: parent}, ctx: ctx, ack: make(chan struct{})}
				switch mode {
				case "acked_live", "acked_fin", "acked_deadline":
					close(r.ack)
				}
				switch mode {
				case "acked_fin", "transport":
					parentCancel(io.EOF)
				case "canceled":
					parentCancel(context.Canceled)
					r.cancelObserved = true
				}
				for pb.Next() {
					r.recordStop()
				}
			})
		})
	}
}

// benchmarkStopReference is the unmodified R9C function, except its name.
func (r *execution) benchmarkStopReference() {
	cause := context.Cause(r.ctx)
	if cause == nil {
		return
	}
	s := r.peer.server
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.ctx.Err() == context.DeadlineExceeded {
		s.stats.Outcomes.RequestDeadlines++
		return
	}
	select {
	case <-r.ack: // Native FIN following an ACK is ordinary transport cleanup.
		return
	default:
	}
	if cause == context.Canceled && r.cancelObserved {
		return
	}
	if cause == ErrClosed {
		s.stats.Outcomes.ShutdownAborts++
	} else if _, ok := cause.(*BudgetError); ok {
		s.stats.Outcomes.BudgetAborts++
	} else if r.peer.ctx.Err() != nil {
		s.stats.Outcomes.TransportAborts++
	}
}

func BenchmarkStopObservationPaired(b *testing.B) {
	for _, mode := range []string{"acked_live", "acked_fin", "acked_deadline", "canceled", "transport"} {
		server := &Server{}
		b.Run(mode, func(b *testing.B) {
			for _, implementation := range []struct {
				name string
				run  func(*execution)
			}{{"reference", (*execution).benchmarkStopReference}, {"runtime", (*execution).recordStop}} {
				b.Run(implementation.name, func(b *testing.B) {
					b.ReportAllocs()
					b.RunParallel(func(pb *testing.PB) {
						parent, cancel := context.WithCancelCause(context.Background())
						defer cancel(nil)
						var ctx context.Context = parent
						if mode == "acked_deadline" {
							var stop context.CancelFunc
							ctx, stop = context.WithDeadline(parent, time.Unix(1, 0))
							defer stop()
						}
						request := &execution{peer: &serverConn{server: server, ctx: parent}, ctx: ctx, ack: make(chan struct{})}
						switch mode {
						case "acked_live", "acked_fin", "acked_deadline":
							close(request.ack)
						}
						switch mode {
						case "acked_fin", "transport":
							cancel(io.EOF)
						case "canceled":
							cancel(context.Canceled)
							request.cancelObserved = true
						}
						for pb.Next() {
							implementation.run(request)
						}
					})
				})
			}
		})
	}
}
