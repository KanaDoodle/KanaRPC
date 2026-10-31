package kanarpc

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func TestTraceRecorderIsBoundedAndNeverWaitsForReader(t *testing.T) {
	r, err := NewTraceRecorder(TraceConfig{Capacity: 16})
	require(t, err)
	for i := 0; i < 40; i++ {
		r.record(TraceEvent{TraceRef: TraceRef{Channel: 1, Request: 1}, Kind: "admitted"})
	}
	v := r.Snapshot(1, 1024)
	if len(v.Events) != 16 || v.Overwritten != 24 || !v.Gap || v.Events[0].Sequence != 25 || v.Last != 40 {
		t.Fatal(v)
	}
	r.mu.Lock()
	// This is intentionally synchronous: a blocking writer would deadlock the test.
	r.record(TraceEvent{Kind: "busy_reader"})
	r.mu.Unlock()
	if r.Snapshot(40, 1).Dropped != 1 {
		t.Fatal("busy journal did not report the skipped event")
	}
	if _, err = NewTraceRecorder(TraceConfig{Capacity: 1 << 20}); err == nil {
		t.Fatal("unbounded journal accepted")
	}
}
func TestTracePreservesReceiptAndCancellationSemantics(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		name := "ack"
		if cancel {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			journal, err := NewTraceRecorder(TraceConfig{Capacity: 128})
			require(t, err)
			refs := make(chan TraceRef, 1)
			s, addr := start(t, ServerConfig{Trace: journal, AckTimeout: time.Second}, func(s *Server) {
				require(t, RegisterUnary(s, "/demo/trace", func(ctx context.Context, in int) (int, error) {
					ref, ok := RequestTrace(ctx)
					if !ok {
						return 0, ErrProtocol
					}
					refs <- ref
					return in, nil
				}))
			})
			c, err := net.Dial("tcp", addr)
			require(t, err)
			defer c.Close()
			rawOpen(t, c, 1, "/demo/trace", false, 1)
			f, end := rawTerminal(t, c)
			if cancel {
				require(t, wire.Write(c, wire.Frame{Kind: wire.Cancel, ID: 1}, wire.HardLimit))
			} else {
				boundaryACK(t, c, f, end)
			}
			eventually(t, func() bool { return s.Stats().Completed == 1 })
			ref := <-refs
			if ref.Channel == 0 || ref.Request != 1 {
				t.Fatal(ref)
			}
			kinds := map[string]bool{}
			for _, e := range journal.Snapshot(0, 128).Events {
				if e.Request == 1 {
					if e.Channel != ref.Channel {
						t.Fatal("request identity changed", e, ref)
					}
					kinds[e.Kind] = true
				}
			}
			for _, kind := range []string{"open", "admitted", "handler_started", "handler_done", "terminal_sent", "retired"} {
				if !kinds[kind] {
					t.Fatal("missing event", kind, kinds)
				}
			}
			if s.Stats().LostTerminals != 0 {
				t.Fatal(s.Stats())
			}
			if cancel {
				if !kinds["cancel_received"] || !kinds["receipt_canceled"] || s.Stats().Outcomes.ReceiptsCanceled != 1 {
					t.Fatal(kinds, s.Stats())
				}
			} else {
				if !kinds["receipt_acknowledged"] || s.Stats().Delivered != 1 {
					t.Fatal(kinds, s.Stats())
				}
			}
		})
	}
}
