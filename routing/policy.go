package routing

import (
	"context"
	"errors"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	rpc "github.com/KanaDoodle/KanaRPC"
)

type SelectionPolicy string

const (
	RoundRobin         SelectionPolicy = "round_robin"
	WeightedRoundRobin SelectionPolicy = "weighted_round_robin"
	LeastOutstanding   SelectionPolicy = "p2c"
)

var ErrCircuitOpen = errors.New("routing: eligible endpoints have open circuits")

type CircuitConfig struct {
	Window, MinSamples int
	FailureRatio       float64
	OpenTimeout        time.Duration
	CountUnavailable   bool
}
type circuitState struct {
	outstanding        int
	current            int64
	samples            []bool
	position, failures int
	openUntil          time.Time
	probe              bool
	generation         uint64
}
type RouteState struct {
	Endpoint          rpc.Endpoint
	Outstanding       int
	Open, Probing     bool
	Samples, Failures int
	Generation        uint64
}

func normalizePolicy(c *Config) error {
	if c.Policy == "" {
		c.Policy = RoundRobin
	}
	if c.Policy != RoundRobin && c.Policy != WeightedRoundRobin && c.Policy != LeastOutstanding {
		return errors.New("routing: invalid selection policy")
	}
	if len(c.Weights) > c.MaxEndpoints {
		return errors.New("routing: too many weights")
	}
	weights := map[string]int{}
	for id, w := range c.Weights {
		if id == "" || len(id) > 128 || w < 1 || w > 10000 {
			return errors.New("routing: invalid endpoint weight")
		}
		weights[id] = w
	}
	c.Weights = weights
	if c.IdleTimeout < 0 {
		return errors.New("routing: invalid idle timeout")
	}
	if c.Circuit != nil {
		v := *c.Circuit
		if v.Window == 0 {
			v.Window = 20
		}
		if v.MinSamples == 0 {
			v.MinSamples = 10
		}
		if v.FailureRatio == 0 {
			v.FailureRatio = .6
		}
		if v.OpenTimeout == 0 {
			v.OpenTimeout = time.Second
		}
		if v.Window < 1 || v.Window > 256 || v.MinSamples < 1 || v.MinSamples > v.Window || v.FailureRatio <= 0 || v.FailureRatio > 1 || v.FailureRatio != v.FailureRatio || v.OpenTimeout < time.Millisecond || v.OpenTimeout > time.Minute {
			return errors.New("routing: invalid circuit policy")
		}
		c.Circuit = &v
	}
	return nil
}
func (c *Client) syncRoutesLocked() {
	if c.cfg.Policy == RoundRobin && c.cfg.Circuit == nil {
		return
	}
	for key := range c.routes {
		if _, ok := c.desired[key]; !ok {
			delete(c.routes, key)
		}
	}
	for key := range c.desired {
		if c.routes[key] == nil {
			c.routes[key] = &circuitState{}
		}
	}
}
func (c *Client) routeEligibleLocked(key rpc.Endpoint, now time.Time) bool {
	if now.Before(c.backoff[key]) {
		return false
	}
	state := c.routes[key]
	if state == nil || c.cfg.Circuit == nil {
		return true
	}
	return !state.probe && !now.Before(state.openUntil)
}
func (c *Client) pickLocked() (rpc.Endpoint, error) {
	now := time.Now()
	if c.cfg.Policy == RoundRobin && c.cfg.Circuit == nil {
		for n := 0; n < len(c.order); n++ {
			key := c.order[c.next%uint64(len(c.order))]
			c.next++
			if !now.Before(c.backoff[key]) {
				return key, nil
			}
		}
		return rpc.Endpoint{}, refusal(ErrBackoff)
	}
	eligible := make([]rpc.Endpoint, 0, len(c.order))
	for _, key := range c.order {
		if c.routeEligibleLocked(key, now) {
			eligible = append(eligible, key)
		}
	}
	if len(eligible) == 0 {
		for _, key := range c.order {
			if !now.Before(c.backoff[key]) {
				c.stats.CircuitRejected++
				return rpc.Endpoint{}, refusal(ErrCircuitOpen)
			}
		}
		return rpc.Endpoint{}, refusal(ErrBackoff)
	}
	switch c.cfg.Policy {
	case WeightedRoundRobin:
		var total int64
		var selected rpc.Endpoint
		best := int64(-1 << 62)
		for _, key := range eligible {
			w := c.cfg.Weights[key.ID]
			if w == 0 {
				w = 1
			}
			state := c.routes[key]
			state.current += int64(w)
			total += int64(w)
			if state.current > best {
				best = state.current
				selected = key
			}
		}
		c.routes[selected].current -= total
		return selected, nil
	case LeastOutstanding:
		a := rand.IntN(len(eligible))
		if len(eligible) == 1 {
			return eligible[a], nil
		}
		b := rand.IntN(len(eligible) - 1)
		if b >= a {
			b++
		}
		x, y := eligible[a], eligible[b]
		wx, wy := c.cfg.Weights[x.ID], c.cfg.Weights[y.ID]
		if wx == 0 {
			wx = 1
		}
		if wy == 0 {
			wy = 1
		}
		if int64(c.routes[x].outstanding+1)*int64(wy) > int64(c.routes[y].outstanding+1)*int64(wx) {
			return y, nil
		}
		return x, nil
	default:
		for n := 0; n < len(c.order); n++ {
			key := c.order[c.next%uint64(len(c.order))]
			c.next++
			if c.routeEligibleLocked(key, now) {
				return key, nil
			}
		}
	}
	return rpc.Endpoint{}, refusal(ErrBackoff)
}

// startRoute counts dispatch-to-protocol-terminal, not connection leases or
// unread application results. No failed business call is replayed here.
func (c *Client) startRoute(e *entry) (func(error), error) {
	if c.cfg.Policy == RoundRobin && c.cfg.Circuit == nil {
		return func(error) {}, nil
	}
	c.mu.Lock()
	state := c.routes[e.key]
	if state == nil {
		c.mu.Unlock()
		return func(error) {}, nil
	}
	if c.cfg.Circuit != nil && (state.probe || time.Now().Before(state.openUntil)) {
		c.stats.CircuitRejected++
		c.mu.Unlock()
		return nil, refusal(ErrCircuitOpen)
	}
	probe := !state.openUntil.IsZero()
	if probe {
		state.probe = true
	}
	generation := state.generation
	state.outstanding++
	c.mu.Unlock()
	var once sync.Once
	return func(err error) {
		once.Do(func() {
			relevant, failure := c.classify(err)
			c.mu.Lock()
			defer c.mu.Unlock()
			state.outstanding--
			if c.routes[e.key] != state || state.generation != generation || c.cfg.Circuit == nil {
				return
			}
			if probe {
				state.probe = false
				if !relevant {
					return
				}
				state.generation++
				if failure {
					c.stats.CircuitOpened++
					state.openUntil = time.Now().Add(c.cfg.Circuit.OpenTimeout)
				} else {
					c.stats.CircuitRecovered++
					state.openUntil = time.Time{}
					state.samples = nil
					state.failures = 0
					state.position = 0
				}
				return
			}
			if !relevant {
				return
			}
			if len(state.samples) < c.cfg.Circuit.Window {
				state.samples = append(state.samples, failure)
			} else {
				if state.samples[state.position] {
					state.failures--
				}
				state.samples[state.position] = failure
				state.position = (state.position + 1) % len(state.samples)
			}
			if failure {
				state.failures++
			}
			if len(state.samples) >= c.cfg.Circuit.MinSamples && float64(state.failures)/float64(len(state.samples)) >= c.cfg.Circuit.FailureRatio {
				state.generation++
				c.stats.CircuitOpened++
				state.openUntil = time.Now().Add(c.cfg.Circuit.OpenTimeout)
			}
		})
	}, nil
}
func (c *Client) classify(err error) (relevant, failure bool) {
	defer func() {
		if recover() != nil {
			relevant = false
			failure = false
		}
	}()
	if err == nil {
		return true, false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false, false
	}
	var b *rpc.BudgetError
	var q *rpc.RateLimitError
	var admission *AdmissionError
	if errors.As(err, &b) || errors.As(err, &q) || errors.As(err, &admission) {
		return false, false
	}
	var status *rpc.Status
	if errors.As(err, &status) {
		return c.cfg.Circuit != nil && c.cfg.Circuit.CountUnavailable && status.Code == rpc.Unavailable, true
	}
	var network net.Error
	return errors.Is(err, rpc.ErrProtocol) || errors.Is(err, rpc.ErrClosed) || errors.As(err, &network), true
}
func (c *Client) RouteStates() []RouteState {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]RouteState, 0, len(c.routes))
	for _, key := range c.order {
		if v := c.routes[key]; v != nil {
			out = append(out, RouteState{key, v.outstanding, !v.openUntil.IsZero(), v.probe, len(v.samples), v.failures, v.generation})
		}
	}
	return out
}

// SetWeights changes routing weights without altering endpoint identity or
// retiring a healthy connection. IDs, not discovery slice positions, are keys.
func (c *Client) SetWeights(weights map[string]int) error {
	c.mu.Lock()
	copy := c.cfg
	c.mu.Unlock()
	copy.Weights = weights
	if e := normalizePolicy(&copy); e != nil {
		return e
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cfg.Weights = copy.Weights
	for _, s := range c.routes {
		s.current = 0
	}
	return nil
}
