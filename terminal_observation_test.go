package kanarpc

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestStopObservationPreservesACKAndDeadlineBoundaries(t *testing.T) {
	for _, name := range []string{"live_ack", "fin_ack", "deadline_ack", "transport_no_ack", "deadline_no_ack", "cancel_no_ack"} {
		t.Run(name, func(t *testing.T) {
			s := &Server{}
			parent, parentCancel := context.WithCancelCause(context.Background())
			defer parentCancel(nil)
			var ctx context.Context = parent
			if name == "deadline_ack" || name == "deadline_no_ack" {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(parent, time.Unix(1, 0))
				defer stop()
			}
			r := &execution{peer: &serverConn{server: s, ctx: parent}, ctx: ctx, ack: make(chan struct{})}
			switch name {
			case "live_ack", "fin_ack", "deadline_ack":
				close(r.ack)
			}
			switch name {
			case "fin_ack", "transport_no_ack":
				parentCancel(io.EOF)
			case "cancel_no_ack":
				parentCancel(context.Canceled)
				r.cancelObserved = true
			}
			r.recordStop()
			v := s.Stats().Outcomes
			wantDeadline := name == "deadline_ack" || name == "deadline_no_ack"
			wantTransport := name == "transport_no_ack"
			if (v.RequestDeadlines == 1) != wantDeadline || (v.TransportAborts == 1) != wantTransport || v.BudgetAborts != 0 || v.ShutdownAborts != 0 {
				t.Fatal(name, v)
			}
		})
	}
}
