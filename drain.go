package kanarpc

import (
	"context"
	"errors"
)

// ServerOutcomes contains cumulative observations, not disjoint RPC totals.
// CancelRequests counts the first CANCEL observed for an active execution,
// even after END; it does not assert cancellation won. Business status codes
// do not increment these counters. Byte refusals live in MemoryStats.Rejected.
type ServerOutcomes struct {
	CapacityRejected, PolicyRejected, DrainingRejected, InvalidRejected, NotFoundRejected uint64
	CancelRequests, RequestDeadlines, TransportAborts, ShutdownAborts, BudgetAborts       uint64
	// ReceiptsResolved equals Delivered + LostTerminals + the two outcomes below.
	// An active receipt wait is not included until it is settled.
	ReceiptsResolved, ReceiptsCanceled, ReceiptsDeadlineExceeded uint64
}

type DeliveryFaults struct{ LostTerminals, IncompleteExchanges uint64 }

func deliveryFaults(stats ServerStats) DeliveryFaults {
	return DeliveryFaults{stats.LostTerminals, stats.IncompleteExchanges}
}

// DrainReport separates resource completion from cumulative delivery health.
// Before is captured under the admission lock when this drain seals admission.
// Quiescent joins framework readers and handlers, not external application work
// or remote consumption/commit. Memory snapshots use their own budget locks;
// they are not atomic with the RPC counters. No history is reset by drain.
type DrainReport struct {
	Before, After             ServerStats
	MemoryBefore, MemoryAfter MemoryStats
	PriorFaults, AddedFaults  DeliveryFaults
	TotalFaults               DeliveryFaults
	Quiescent                 bool
}

// DrainWithReport retains both a context failure and ErrDelivery when present.
// With only cumulative delivery faults it returns ErrDelivery, as Drain does.
func (s *Server) DrainWithReport(ctx context.Context) (report DrainReport, err error) {
	s.mu.Lock()
	report.Before = s.stats
	if s.state == fresh {
		s.state = closed
	}
	if s.state == serving {
		s.state = draining
	}
	l := s.listener
	s.notifyLocked()
	s.mu.Unlock()
	report.MemoryBefore = s.MemoryStats()
	report.PriorFaults = deliveryFaults(report.Before)
	s.trace(TraceRef{}, "drain_started", "new_admission_sealed", OK, 0)
	defer func() {
		s.mu.Lock()
		report.After = s.stats
		report.Quiescent = !s.accepting && s.stats.Connections == 0 && s.stats.InFlight == 0 && s.stats.NativeStreams == 0
		s.mu.Unlock()
		report.MemoryAfter = s.MemoryStats()
		report.TotalFaults = deliveryFaults(report.After)
		report.AddedFaults = DeliveryFaults{report.TotalFaults.LostTerminals - report.PriorFaults.LostTerminals, report.TotalFaults.IncompleteExchanges - report.PriorFaults.IncompleteExchanges}
		if report.TotalFaults.LostTerminals != 0 || report.TotalFaults.IncompleteExchanges != 0 {
			if err == nil {
				err = ErrDelivery
			} else {
				err = errors.Join(err, ErrDelivery)
			}
		}
		if s.cfg.Trace != nil {
			code := OK
			if err != nil {
				code = statusOf(err).Code
			}
			s.trace(TraceRef{}, "drain_finished", "snapshot_not_business_commit", code, int64(report.TotalFaults.LostTerminals))
		}
	}()
	if l != nil {
		_ = l.Close()
	}
	for {
		s.mu.Lock()
		quiet := !s.accepting && s.stats.InFlight == 0
		changed := s.changed
		s.mu.Unlock()
		if quiet {
			break
		}
		select {
		case <-ctx.Done():
			s.Close()
			return report, ctx.Err()
		case <-changed:
		}
	}
	s.Close()
	return report, s.Wait(ctx)
}

func (r *execution) cancelRequest() {
	s := r.peer.server
	s.mu.Lock()
	if !r.cancelObserved {
		r.cancelObserved = true
		s.stats.Outcomes.CancelRequests++
		r.trace("cancel_received", "first_cancel_control", Canceled, 0)
	}
	r.cancel(context.Canceled)
	s.mu.Unlock()
}

// Called before cleanup's artificial ErrClosed cancellation, once per run.
func (r *execution) recordStop() {
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
