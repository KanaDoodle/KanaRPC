package kanarpc

import (
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"time"
)

// Pressure is advisory metadata from an authenticated/configured peer, not a
// distributed lease, reservation, tenant identity or authorization decision.
type Pressure struct {
	Version                                          int    `json:"v"`
	Epoch                                            string `json:"epoch"`
	Sequence                                         uint64 `json:"seq"`
	TTLMillis                                        int64  `json:"ttl_ms"`
	Method                                           string `json:"method"`
	Limit, Running, Queued, PeersDemand, Recommended int
	ReceiveUsed, ReceiveLimit                        int64
}
type FeedbackConfig struct {
	MaxAge      time.Duration
	MaxMethods  int
	ObserveOnly bool
}
type FeedbackMethod struct {
	Method              string
	Active, Recommended int
	Sequence            uint64
	Fresh               bool
}
type FeedbackStats struct {
	Enabled                    bool
	Accepted, Ignored, Limited uint64
	Methods                    []FeedbackMethod
}
type feedbackEntry struct {
	pressure Pressure
	observed time.Time
	active   int
	validAge time.Duration
}
type feedbackTracker struct {
	mu                         sync.Mutex
	cfg                        FeedbackConfig
	cap                        int
	epoch                      string
	methods                    map[string]*feedbackEntry
	accepted, ignored, limited uint64
}

func normalizeFeedback(c *FeedbackConfig) (*FeedbackConfig, error) {
	if c == nil {
		return nil, nil
	}
	copy := *c
	if copy.MaxAge == 0 {
		copy.MaxAge = time.Second
	}
	if copy.MaxMethods == 0 {
		copy.MaxMethods = 128
	}
	if copy.MaxAge < 50*time.Millisecond || copy.MaxAge > 5*time.Second || copy.MaxMethods < 1 || copy.MaxMethods > 4096 {
		return nil, errors.New("kanarpc: invalid feedback bounds")
	}
	return &copy, nil
}
func newFeedback(c *FeedbackConfig, cap int) *feedbackTracker {
	if c == nil {
		return nil
	}
	return &feedbackTracker{cfg: *c, cap: cap, methods: make(map[string]*feedbackEntry)}
}
func (f *feedbackTracker) entryLocked(path string) *feedbackEntry {
	if entry := f.methods[path]; entry != nil {
		return entry
	}
	if len(f.methods) >= f.cfg.MaxMethods {
		for key, entry := range f.methods {
			if entry.active == 0 && !f.fresh(entry) {
				delete(f.methods, key)
				break
			}
		}
	}
	if len(f.methods) >= f.cfg.MaxMethods {
		return nil
	}
	entry := &feedbackEntry{}
	f.methods[path] = entry
	return entry
}
func (f *feedbackTracker) fresh(e *feedbackEntry) bool {
	age := time.Since(e.observed)
	return !e.observed.IsZero() && age >= 0 && age < e.validAge && age < f.cfg.MaxAge
}
func (f *feedbackTracker) enter(path string) (func(), error) {
	if f == nil {
		return func() {}, nil
	}
	f.mu.Lock()
	entry := f.entryLocked(path)
	if entry == nil {
		f.mu.Unlock()
		return func() {}, nil
	} // bounded metadata; local static limit still applies
	limit := f.cap
	if f.fresh(entry) && entry.pressure.Recommended < limit {
		limit = entry.pressure.Recommended
	}
	if !f.cfg.ObserveOnly && entry.active >= limit {
		f.limited++
		f.mu.Unlock()
		return nil, &Status{Code: ResourceExhausted, Message: "local pressure admission"}
	}
	entry.active++
	f.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { f.mu.Lock(); entry.active--; f.mu.Unlock() }) }, nil
}
func (f *feedbackTracker) observe(path string, p *Pressure, requestAge ...time.Duration) {
	if f == nil || p == nil {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	epoch, err := hex.DecodeString(p.Epoch)
	valid := err == nil && len(epoch) == 16 && p.Version == 1 && p.Sequence > 0 && p.Method == path && p.TTLMillis >= 50 && p.TTLMillis <= f.cfg.MaxAge.Milliseconds() && p.Limit >= 1 && p.Limit <= 4096 && p.Running >= 0 && p.Running <= 4096 && p.Queued >= 0 && p.Queued <= 4096 && p.PeersDemand >= 1 && p.PeersDemand <= 4096 && p.Recommended >= 1 && p.Recommended <= p.Limit && p.ReceiveUsed >= 0 && p.ReceiveLimit >= p.ReceiveUsed && p.ReceiveLimit <= 1<<40
	if !valid || f.epoch != "" && f.epoch != p.Epoch {
		f.ignored++
		return
	}
	age := time.Duration(0)
	if len(requestAge) > 0 {
		age = requestAge[0]
	}
	ttl := time.Duration(p.TTLMillis) * time.Millisecond
	if age < 0 || age >= ttl {
		f.ignored++
		return
	}
	entry := f.entryLocked(path)
	if entry == nil || p.Sequence <= entry.pressure.Sequence {
		f.ignored++
		return
	}
	if f.epoch == "" {
		f.epoch = p.Epoch
	}
	entry.pressure = *p
	entry.observed = time.Now()
	entry.validAge = ttl - age
	f.accepted++
}
func (f *feedbackTracker) snapshot() FeedbackStats {
	if f == nil {
		return FeedbackStats{}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	result := FeedbackStats{Enabled: true, Accepted: f.accepted, Ignored: f.ignored, Limited: f.limited}
	for path, entry := range f.methods {
		result.Methods = append(result.Methods, FeedbackMethod{path, entry.active, entry.pressure.Recommended, entry.pressure.Sequence, f.fresh(entry)})
	}
	sort.Slice(result.Methods, func(i, j int) bool { return result.Methods[i].Method < result.Methods[j].Method })
	return result
}
func (c *Client) FeedbackStats() FeedbackStats     { return c.resources.feedback.snapshot() }
func (c *QUICClient) FeedbackStats() FeedbackStats { return c.resources.feedback.snapshot() }
func (s *Server) pressure(p *serverConn, path string) *Pressure {
	s.mu.Lock()
	defer s.mu.Unlock()
	g := s.governor
	if g == nil || !g.cfg.Feedback {
		return nil
	}
	method := g.methods[path]
	if method == nil {
		return nil
	}
	peers := 0
	selfIncluded := false
	for peer := range s.connections {
		if peer.demand[path] > 0 {
			peers++
			if peer == p {
				selfIncluded = true
			}
		}
	}
	for peer := range s.nativeSessions {
		if peer.demand[path] > 0 {
			peers++
			if peer == p.native {
				selfIncluded = true
			}
		}
	}
	if !selfIncluded {
		peers++
	}
	if peers < 1 {
		peers = 1
	}
	share := method.limit / peers
	if share < 1 {
		share = 1
	}
	g.sequence++
	memory := s.resources.receive.snapshot()
	return &Pressure{Version: 1, Epoch: g.epoch, Sequence: g.sequence, TTLMillis: g.cfg.FeedbackTTL.Milliseconds(), Method: path, Limit: method.limit, Running: method.running, Queued: method.queued, PeersDemand: peers, Recommended: share, ReceiveUsed: memory.Used, ReceiveLimit: memory.Limit}
}
