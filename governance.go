package kanarpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"
)

type QueuePolicy string

const (
	FIFO       QueuePolicy = "fifo"
	MethodFair QueuePolicy = "method_fair"
)

type MethodPolicy struct {
	Method                    string
	Weight, Min, Initial, Max int
	Target                    time.Duration
	// FallbackLimit=0 disables the guard. Otherwise sustained successful
	// windows above Target at Min latch this operator-chosen fixed limit.
	FallbackLimit, FallbackWindows int
}

type ControlMode string

const (
	ControlStatic   ControlMode = "static"
	ControlAdaptive ControlMode = "adaptive"
	ControlFallback ControlMode = "fixed_fallback"
)

type GovernanceConfig struct {
	MaxExecuting, QueueCapacity, SampleWindow int
	QueueTimeout, FeedbackTTL                 time.Duration
	QueuePolicy                               QueuePolicy
	Adaptive, Feedback                        bool
	Methods                                   []MethodPolicy
}
type MethodState struct {
	Method                         string
	Limit, Running, Queued, Weight int
	Samples                        uint64
	P90                            time.Duration
	Mode                           ControlMode
	UnmetWindows                   int
	Generation, Fallbacks, Resets  uint64
}
type ControlDecision struct {
	Method, Reason         string
	Previous, Next         int
	P90                    time.Duration
	PreviousMode, NextMode ControlMode
	Generation             uint64
}
type GovernanceSnapshot struct {
	Enabled, Adaptive bool
	Epoch             string
	Executing, Queued int
	Methods           []MethodState
	Decisions         []ControlDecision
	DroppedDecisions  uint64
}
type methodControl struct {
	policy                        MethodPolicy
	limit, running, queued        int
	samples                       uint64
	window                        []time.Duration
	saturated, bad                bool
	p90                           time.Duration
	mode                          ControlMode
	unmet                         int
	failed                        bool
	generation, fallbacks, resets uint64
}
type governor struct {
	cfg            GovernanceConfig
	methods        map[string]*methodControl
	order          []string
	queue          []*execution
	cursor, credit int
	epoch          string
	sequence       uint64
	decisions      []ControlDecision
	dropped        uint64
}

func normalizeGovernance(input *GovernanceConfig, inflight int) (*GovernanceConfig, error) {
	if input == nil {
		return nil, nil
	}
	c := *input
	c.Methods = append([]MethodPolicy(nil), input.Methods...)
	if c.MaxExecuting == 0 {
		c.MaxExecuting = 16
	}
	if c.QueueCapacity == 0 {
		c.QueueCapacity = 64
	}
	if c.QueueTimeout == 0 {
		c.QueueTimeout = 100 * time.Millisecond
	}
	if c.SampleWindow == 0 {
		c.SampleWindow = 16
	}
	if c.FeedbackTTL == 0 {
		c.FeedbackTTL = 500 * time.Millisecond
	}
	if c.QueuePolicy == "" {
		c.QueuePolicy = MethodFair
	}
	if c.MaxExecuting < 1 || c.MaxExecuting > inflight || c.QueueCapacity < 0 || c.QueueCapacity > inflight || c.QueueTimeout <= 0 || c.SampleWindow < 4 || c.SampleWindow > 256 || c.FeedbackTTL < 50*time.Millisecond || c.FeedbackTTL > 5*time.Second || (c.QueuePolicy != FIFO && c.QueuePolicy != MethodFair) || len(c.Methods) > 1024 {
		return nil, errors.New("kanarpc: invalid governance bounds")
	}
	seen := map[string]bool{}
	for i, p := range c.Methods {
		if !validMethod(p.Method) || seen[p.Method] {
			return nil, errors.New("kanarpc: invalid or duplicate governed method")
		}
		seen[p.Method] = true
		if p.Weight == 0 {
			p.Weight = 1
		}
		if p.Min == 0 {
			p.Min = 1
		}
		if p.Max == 0 {
			p.Max = c.MaxExecuting
		}
		if p.Initial == 0 {
			p.Initial = p.Max
		}
		if p.Target == 0 {
			p.Target = 20 * time.Millisecond
		}
		if p.Weight < 1 || p.Weight > 16 || p.Min < 1 || p.Min > p.Initial || p.Initial > p.Max || p.Max > c.MaxExecuting || p.Target <= 0 {
			return nil, errors.New("kanarpc: invalid method policy")
		}
		if p.FallbackLimit == 0 {
			if p.FallbackWindows != 0 {
				return nil, errors.New("kanarpc: fallback windows require a fixed fallback limit")
			}
		} else {
			if p.FallbackWindows == 0 {
				p.FallbackWindows = 3
			}
			if !c.Adaptive || p.FallbackLimit < p.Min || p.FallbackLimit > p.Max || p.FallbackWindows < 1 || p.FallbackWindows > 64 {
				return nil, errors.New("kanarpc: invalid adaptive fallback policy")
			}
		}
		c.Methods[i] = p
	}
	return &c, nil
}
func newGovernor(c *GovernanceConfig) (*governor, error) {
	if c == nil {
		return nil, nil
	}
	var epoch [16]byte
	if _, err := rand.Read(epoch[:]); err != nil {
		return nil, err
	}
	return &governor{cfg: *c, epoch: hex.EncodeToString(epoch[:]), methods: make(map[string]*methodControl)}, nil
}
func (g *governor) add(path string) {
	p := MethodPolicy{Method: path, Weight: 1, Min: 1, Initial: g.cfg.MaxExecuting, Max: g.cfg.MaxExecuting, Target: 20 * time.Millisecond}
	for _, candidate := range g.cfg.Methods {
		if candidate.Method == path {
			p = candidate
			break
		}
	}
	mode := ControlStatic
	if g.cfg.Adaptive {
		mode = ControlAdaptive
	}
	g.methods[path] = &methodControl{policy: p, limit: p.Initial, mode: mode}
	g.order = append(g.order, path)
	sort.Strings(g.order)
}
func (s *Server) validatePoliciesLocked() error {
	if s.governor != nil {
		for _, policy := range s.governor.cfg.Methods {
			if _, ok := s.methods[policy.Method]; !ok {
				return fmt.Errorf("kanarpc: policy for unregistered method %s", policy.Method)
			}
		}
	}
	return nil
}
func (g *governor) removeAt(index int) {
	copy(g.queue[index:], g.queue[index+1:])
	g.queue[len(g.queue)-1] = nil
	g.queue = g.queue[:len(g.queue)-1]
}
func (s *Server) Governance() GovernanceSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := GovernanceSnapshot{Executing: s.stats.Executing, Queued: s.stats.Queued}
	g := s.governor
	if g == nil {
		return snapshot
	}
	snapshot.Enabled = true
	snapshot.Adaptive = g.cfg.Adaptive
	snapshot.Epoch = g.epoch
	snapshot.DroppedDecisions = g.dropped
	for _, path := range g.order {
		m := g.methods[path]
		snapshot.Methods = append(snapshot.Methods, MethodState{Method: path, Limit: m.limit, Running: m.running, Queued: m.queued, Weight: m.policy.Weight, Samples: m.samples, P90: m.p90, Mode: m.mode, UnmetWindows: m.unmet, Generation: m.generation, Fallbacks: m.fallbacks, Resets: m.resets})
	}
	snapshot.Decisions = append([]ControlDecision(nil), g.decisions...)
	return snapshot
}

func (g *governor) recordDecision(d ControlDecision) {
	if len(g.decisions) == 128 {
		copy(g.decisions, g.decisions[1:])
		g.decisions = g.decisions[:127]
		g.dropped++
	}
	g.decisions = append(g.decisions, d)
}

// ResetAdaptive is a local operator action, never an automatic business retry.
// Existing handlers keep their slots but cannot sample into the new generation.
func (s *Server) ResetAdaptive(path string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == closed {
		return ErrClosed
	}
	if s.state == draining {
		return ErrDraining
	}
	if s.governor == nil || !s.governor.cfg.Adaptive {
		return &Status{Code: InvalidArgument, Message: "adaptive control is disabled"}
	}
	m := s.governor.methods[path]
	if m == nil {
		return &Status{Code: NotFound, Message: "method is not registered"}
	}
	old, previous, p90 := m.limit, m.mode, m.p90
	m.generation++
	m.resets++
	m.limit, m.mode = m.policy.Initial, ControlAdaptive
	m.window = m.window[:0]
	m.saturated, m.bad, m.failed = false, false, false
	m.unmet, m.p90 = 0, 0
	s.governor.recordDecision(ControlDecision{Method: path, Reason: "operator_reset", Previous: old, Next: m.limit, P90: p90, PreviousMode: previous, NextMode: m.mode, Generation: m.generation})
	s.dispatchLocked()
	s.notifyLocked()
	return nil
}
func (s *Server) canRunLocked(path string) bool {
	g := s.governor
	m := g.methods[path]
	return s.stats.Executing < g.cfg.MaxExecuting && m.running < m.limit
}
func (s *Server) canAdmitLocked(path string) bool {
	g := s.governor
	return len(g.queue) < g.cfg.QueueCapacity || s.canRunLocked(path)
}
func (s *Server) enqueueLocked(r *execution, h method, body []byte) {
	g := s.governor
	r.handler = h
	r.body = body
	r.queuedAt = time.Now()
	r.jobState = jobQueued
	if len(g.queue) >= g.cfg.QueueCapacity && s.canRunLocked(r.path) {
		g.methods[r.path].queued++
		s.stats.Queued++
		s.startLocked(r)
		return
	}
	g.queue = append(g.queue, r)
	g.methods[r.path].queued++
	s.stats.Queued++
	queueContext, stop := context.WithTimeout(r.ctx, g.cfg.QueueTimeout)
	r.queueCancel = stop
	r.queueDeadline, _ = queueContext.Deadline()
	r.queueStop = context.AfterFunc(queueContext, func() { s.cancelQueued(r, queueContext.Err()) })
	s.dispatchLocked()
}

const (
	jobQueued uint8 = iota + 1
	jobRunning
	jobFinished
)

func (s *Server) startLocked(r *execution) {
	g := s.governor
	m := g.methods[r.path]
	r.jobState = jobRunning
	r.started = time.Now()
	r.queueWait = r.started.Sub(r.queuedAt)
	if r.queueStop != nil {
		r.queueStop()
		r.queueCancel()
	}
	m.running++
	r.controlGeneration = m.generation
	m.queued--
	s.stats.Queued--
	s.stats.Executing++
	if m.running*2 >= m.limit || m.queued > 0 {
		m.saturated = true
	}
	go r.run(r.handler, r.body)
}
func (s *Server) removeQueuedLocked(index int, err error) {
	g := s.governor
	r := g.queue[index]
	g.removeAt(index)
	g.methods[r.path].queued--
	s.stats.Queued--
	r.jobState = jobFinished
	r.queueError = err
	if r.queueStop != nil {
		r.queueStop()
		r.queueCancel()
	}
	s.demandLocked(r.peer, r.path, -1)
	go r.run(method{}, nil)
}
func (s *Server) cancelQueued(r *execution, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.jobState != jobQueued {
		return
	}
	for i, candidate := range s.governor.queue {
		if candidate == r {
			s.removeQueuedLocked(i, err)
			s.dispatchLocked()
			s.notifyLocked()
			return
		}
	}
}
func (s *Server) dispatchLocked() {
	g := s.governor
	if s.state == closed {
		return
	}
	for i := 0; i < len(g.queue); {
		r := g.queue[i]
		err := deadlineError(r.ctx)
		if err == nil && !time.Now().Before(r.queueDeadline) {
			err = context.DeadlineExceeded
		}
		if err != nil {
			s.removeQueuedLocked(i, err)
			continue
		}
		i++
	}
	for len(g.queue) > 0 && s.stats.Executing < g.cfg.MaxExecuting {
		index := -1
		if g.cfg.QueuePolicy == FIFO {
			for i, r := range g.queue {
				if s.canRunLocked(r.path) {
					index = i
					break
				}
			}
		} else {
			for attempts := 0; attempts < len(g.order); attempts++ {
				path := g.order[g.cursor]
				if g.credit == 0 {
					g.credit = g.methods[path].policy.Weight
				}
				for i, r := range g.queue {
					if r.path == path && s.canRunLocked(path) {
						index = i
						break
					}
				}
				if index >= 0 {
					g.credit--
					if g.credit == 0 {
						g.cursor = (g.cursor + 1) % len(g.order)
					}
					break
				}
				g.credit = 0
				g.cursor = (g.cursor + 1) % len(g.order)
			}
		}
		if index < 0 {
			return
		}
		r := g.queue[index]
		g.removeAt(index)
		s.startLocked(r)
	}
}
func (s *Server) handlerDoneLocked(r *execution, status Status, duration time.Duration) {
	s.stats.Executing--
	s.demandLocked(r.peer, r.path, -1)
	g := s.governor
	if g == nil {
		return
	}
	m := g.methods[r.path]
	m.running--
	r.jobState = jobFinished
	if g.cfg.Adaptive && status.Code != Canceled && r.controlGeneration == m.generation {
		if status.Code != OK {
			m.failed = true
			m.unmet = 0
		}
		eligible := status.Code == OK || status.Code == ResourceExhausted || status.Code == Unavailable || status.Code == DeadlineExceeded && duration >= m.policy.Target
		if eligible {
			m.samples++
			m.window = append(m.window, duration)
			if status.Code != OK {
				m.bad = true
			}
			if len(m.window) >= g.cfg.SampleWindow {
				sort.Slice(m.window, func(i, j int) bool { return m.window[i] < m.window[j] })
				m.p90 = m.window[(len(m.window)-1)*9/10]
				old := m.limit
				previous := m.mode
				reason := "hold"
				if m.mode == ControlAdaptive && m.policy.FallbackLimit > 0 {
					if old == m.policy.Min && m.p90 > m.policy.Target && !m.failed {
						m.unmet++
					} else {
						m.unmet = 0
					}
				}
				if m.mode == ControlAdaptive && m.policy.FallbackLimit > 0 && m.unmet >= m.policy.FallbackWindows {
					m.limit, m.mode = m.policy.FallbackLimit, ControlFallback
					m.fallbacks++
					reason = "target_unmet_fallback"
				} else if m.mode == ControlAdaptive && (m.bad || m.p90 > m.policy.Target) {
					m.limit = m.limit * 4 / 5
					if m.limit < m.policy.Min {
						m.limit = m.policy.Min
					}
					reason = "latency_or_overload"
				} else if m.mode == ControlAdaptive && m.saturated {
					m.limit++
					if m.limit > m.policy.Max {
						m.limit = m.policy.Max
					}
					reason = "demand_growth"
				}
				if old != m.limit || previous != m.mode {
					g.recordDecision(ControlDecision{Method: r.path, Reason: reason, Previous: old, Next: m.limit, P90: m.p90, PreviousMode: previous, NextMode: m.mode, Generation: m.generation})
				}
				m.window = m.window[:0]
				m.saturated = false
				m.bad = false
				m.failed = false
			}
		}
	}
	s.dispatchLocked()
}
func (s *Server) demandLocked(p *serverConn, path string, delta int) {
	table := &p.demand
	if p.native != nil {
		table = &p.native.demand
	}
	if *table == nil {
		*table = make(map[string]int)
	}
	(*table)[path] += delta
	if (*table)[path] <= 0 {
		delete(*table, path)
	}
}
