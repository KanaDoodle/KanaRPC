package kanarpc

import (
	"context"
	"encoding/json"
	"github.com/KanaDoodle/KanaRPC/internal/wire"
	"math"
	"net"
	"sync"
	"testing"
	"time"
)

func validPressure(path string, sequence uint64) Pressure {
	return Pressure{Version: 1, Epoch: "00112233445566778899aabbccddeeff", Sequence: sequence, TTLMillis: 500, Method: path, Limit: 8, PeersDemand: 2, Recommended: 4, ReceiveLimit: 1024}
}

type terminalGateChannel struct {
	net.Conn
	entered, release chan struct{}
}

func (c *terminalGateChannel) AwaitEnd(context.Context) error {
	close(c.entered)
	<-c.release
	return nil
}
func (c *terminalGateChannel) Finish() {}
func TestTerminalCompletionReleasesFeedbackBeforeRetirement(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	entered, release := make(chan struct{}), make(chan struct{})
	var unlock sync.Once
	locked := false
	c, err := newRecordClient(&terminalGateChannel{left, entered, release}, ClientConfig{Feedback: &FeedbackConfig{}}, true)
	require(t, err)
	t.Cleanup(func() {
		if locked {
			unlock.Do(func() { c.mu.Unlock() })
		}
		c.Close()
	})
	go func() {
		frame, e := wire.Read(right, wire.HardLimit)
		if e != nil {
			return
		}
		go func() {
			for {
				if _, e := wire.Read(right, wire.HardLimit); e != nil {
					return
				}
			}
		}()
		p := validPressure("/demo/echo", 1)
		end, _ := json.Marshal(endRecord{Status: Status{Code: OK}, Pressure: &p})
		_ = wire.Write(right, wire.Frame{Kind: wire.Data, ID: frame.ID, Number: 1, Payload: []byte("1")}, wire.HardLimit)
		_ = wire.Write(right, wire.Frame{Kind: wire.End, ID: frame.ID, Payload: end}, wire.HardLimit)
	}()
	done := make(chan error, 1)
	go func() { var out int; done <- c.Call(context.Background(), "/demo/echo", 1, &out) }()
	signal(t, entered)
	c.mu.Lock()
	locked = true
	close(release)
	require(t, <-done)
	if state := c.FeedbackStats(); len(state.Methods) != 1 || state.Methods[0].Active != 0 {
		t.Fatal("completed call retained advisory permit until IO retirement", state)
	}
	unlock.Do(func() { c.mu.Unlock() })
}
func TestDelayedPressureCannotRestartFullTTLAtReceipt(t *testing.T) {
	left, right := net.Pipe()
	defer right.Close()
	c, err := NewClient(left, ClientConfig{Feedback: &FeedbackConfig{}})
	require(t, err)
	defer c.Close()
	go func() {
		frame, e := wire.Read(right, wire.HardLimit)
		if e != nil {
			return
		}
		p := validPressure("/demo/echo", 1)
		p.TTLMillis = 50
		end, _ := json.Marshal(endRecord{Status: Status{Code: OK}, Pressure: &p})
		time.Sleep(80 * time.Millisecond)
		_ = wire.Write(right, wire.Frame{Kind: wire.Data, ID: frame.ID, Number: 1, Payload: []byte("1")}, wire.HardLimit)
		_ = wire.Write(right, wire.Frame{Kind: wire.End, ID: frame.ID, Payload: end}, wire.HardLimit)
		for {
			if _, e = wire.Read(right, wire.HardLimit); e != nil {
				return
			}
		}
	}()
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 1, &out))
	if state := c.FeedbackStats(); state.Accepted != 0 {
		t.Fatal("delayed snapshot acquired a fresh full TTL", state)
	}
}
func TestFeedbackSequenceScopeExpiryAndEpoch(t *testing.T) {
	f := newFeedback(&FeedbackConfig{MaxAge: time.Second, MaxMethods: 2}, 64)
	a, b := validPressure("/demo/a", 10), validPressure("/demo/b", 1)
	f.observe(a.Method, &a)
	f.observe(b.Method, &b)
	f.observe(a.Method, &a)
	foreign := a
	foreign.Epoch = "ffeeddccbbaa99887766554433221100"
	foreign.Sequence++
	f.observe(a.Method, &foreign)
	if state := f.snapshot(); state.Accepted != 2 || state.Ignored != 2 {
		t.Fatal(state)
	}
	var release []func()
	for i := 0; i < 4; i++ {
		r, e := f.enter(a.Method)
		require(t, e)
		release = append(release, r)
	}
	if _, e := f.enter(a.Method); e == nil {
		t.Fatal("fresh feedback not enforced")
	}
	f.mu.Lock()
	f.methods[a.Method].observed = time.Now().Add(-time.Second)
	f.mu.Unlock()
	r, e := f.enter(a.Method)
	require(t, e)
	r()
	for _, done := range release {
		done()
	}
	for _, m := range f.snapshot().Methods {
		if m.Active != 0 {
			t.Fatal(m)
		}
	}
}
func TestFeedbackRejectsTTLOverflowAndMalformedPressure(t *testing.T) {
	for _, ttl := range []int64{math.MaxInt64, 1 << 62, 9223372036855} {
		f := newFeedback(&FeedbackConfig{MaxAge: time.Second, MaxMethods: 2}, 64)
		p := validPressure("/demo/a", 1)
		p.TTLMillis = ttl
		f.observe(p.Method, &p)
		if state := f.snapshot(); state.Accepted != 0 {
			t.Fatal("overflow TTL accepted", ttl, state)
		}
	}
}
func FuzzPressure(f *testing.F) {
	f.Add(int64(500), uint64(1), 8, 4, "00112233445566778899aabbccddeeff")
	f.Fuzz(func(t *testing.T, ttl int64, seq uint64, limit, recommend int, epoch string) {
		tracker := newFeedback(&FeedbackConfig{MaxAge: time.Second, MaxMethods: 2}, 64)
		p := validPressure("/demo/a", seq)
		p.TTLMillis = ttl
		p.Limit = limit
		p.Recommended = recommend
		p.Epoch = epoch
		tracker.observe(p.Method, &p)
		if tracker.snapshot().Accepted > 0 && (ttl < 50 || ttl > 1000 || limit < 1 || limit > 4096 || recommend < 1 || recommend > limit) {
			t.Fatal("invalid feedback admitted", p)
		}
	})
}
