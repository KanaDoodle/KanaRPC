package kanarpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// TraceRecorder is an opt-in, bounded local decision journal, not a distributed
// trace or delivery ledger. Writers never wait for the reader or call user code.
// Dropped/overwritten events make incomplete timelines explicit.
type TraceConfig struct {
	Capacity    int
	SampleEvery uint64 // deterministic sampling by local channel/request pair
}
type TraceRef struct {
	Channel uint64 `json:"channel"`
	Request uint64 `json:"request"`
	Method  string `json:"method"`
}
type TraceEvent struct {
	TraceRef
	Sequence uint64    `json:"sequence"`
	At       time.Time `json:"at"`
	Kind     string    `json:"kind"`
	Reason   string    `json:"reason,omitempty"`
	Code     Code      `json:"code,omitempty"`
	Value    int64     `json:"value,omitempty"`
	Next     int64     `json:"next,omitempty"`
}
type TraceSnapshot struct {
	Events      []TraceEvent `json:"events"`
	Capacity    int          `json:"capacity"`
	SampleEvery uint64       `json:"sample_every"`
	Last        uint64       `json:"last"`
	Oldest      uint64       `json:"oldest"`
	Dropped     uint64       `json:"dropped"`
	Overwritten uint64       `json:"overwritten"`
	SampledOut  uint64       `json:"sampled_out"`
	Gap         bool         `json:"gap"`
}
type TraceRecorder struct {
	mu                 sync.Mutex
	events             []TraceEvent
	head, count        int
	sequence, replaced uint64
	sampleEvery        uint64
	channels, dropped  atomic.Uint64
	sampledOut         atomic.Uint64
}

func NewTraceRecorder(c TraceConfig) (*TraceRecorder, error) {
	if c.Capacity == 0 {
		c.Capacity = 4096
	}
	if c.SampleEvery == 0 {
		c.SampleEvery = 1
	}
	if c.Capacity < 16 || c.Capacity > 16384 || c.SampleEvery > 1000000 {
		return nil, errors.New("kanarpc: invalid decision journal bounds")
	}
	return &TraceRecorder{events: make([]TraceEvent, c.Capacity), sampleEvery: c.SampleEvery}, nil
}
func (r *TraceRecorder) newChannel() uint64 {
	if r == nil {
		return 0
	}
	return r.channels.Add(1)
}
func (r *TraceRecorder) record(e TraceEvent) {
	if e.Request != 0 && ((e.Channel*0x9e3779b97f4a7c15)^e.Request)%r.sampleEvery != 0 {
		r.sampledOut.Add(1)
		return
	}
	if !r.mu.TryLock() {
		r.dropped.Add(1)
		return
	}
	defer r.mu.Unlock()
	r.sequence++
	e.Sequence, e.At = r.sequence, time.Now()
	index := (r.head + r.count) % len(r.events)
	if r.count == len(r.events) {
		r.head = (r.head + 1) % len(r.events)
		r.replaced++
	} else {
		r.count++
	}
	r.events[index] = e
}

// Snapshot copies at most 1024 events. RPC writers skip a busy journal instead
// of waiting behind this copy. Memory is fixed at construction; no body is kept.
func (r *TraceRecorder) Snapshot(after uint64, limit int) TraceSnapshot {
	if limit < 1 || limit > 1024 {
		limit = 512
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := TraceSnapshot{Capacity: len(r.events), SampleEvery: r.sampleEvery, Last: r.sequence, Dropped: r.dropped.Load(), Overwritten: r.replaced, SampledOut: r.sampledOut.Load()}
	if r.count > 0 {
		s.Oldest = r.events[r.head].Sequence
		s.Gap = after != 0 && after < s.Oldest-1
	}
	for i := 0; i < r.count && len(s.Events) < limit; i++ {
		e := r.events[(r.head+i)%len(r.events)]
		if e.Sequence > after {
			s.Events = append(s.Events, e)
		}
	}
	return s
}

type requestTraceKey struct{}

// RequestTrace returns local identity only when the server journal is enabled.
// It contains no propagated user identity and grants no permissions.
func RequestTrace(ctx context.Context) (TraceRef, bool) {
	r, ok := ctx.Value(requestTraceKey{}).(TraceRef)
	return r, ok
}
func (s *Server) trace(ref TraceRef, kind, reason string, code Code, value int64) {
	if s.cfg.Trace != nil {
		s.cfg.Trace.record(TraceEvent{TraceRef: ref, Kind: kind, Reason: reason, Code: code, Value: value})
	}
}
func (p *serverConn) trace(id uint64, method, kind, reason string, code Code, value int64) {
	p.server.trace(TraceRef{p.traceChannel, id, method}, kind, reason, code, value)
}
func (r *execution) trace(kind, reason string, code Code, value int64) {
	r.peer.trace(r.id, r.path, kind, reason, code, value)
}
