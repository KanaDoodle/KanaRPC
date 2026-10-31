package kanarpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func controlFixture(t testing.TB, fallback int) (*Server, func(Code, time.Duration, bool)) {
	t.Helper()
	s, err := NewServer(ServerConfig{Governance: &GovernanceConfig{MaxExecuting: 8, SampleWindow: 4, Adaptive: true, Methods: []MethodPolicy{{Method: "/demo/echo", Min: 2, Initial: 8, Max: 8, Target: 5 * time.Millisecond, FallbackLimit: fallback, FallbackWindows: map[bool]int{true: 2}[fallback > 0]}}}})
	require(t, err)
	echo(s)
	return s, func(code Code, duration time.Duration, stale bool) {
		s.mu.Lock()
		defer s.mu.Unlock()
		m := s.governor.methods["/demo/echo"]
		gen := m.generation
		if stale && gen > 0 {
			gen--
		}
		r := &execution{path: "/demo/echo", peer: &serverConn{server: s}, controlGeneration: gen}
		s.stats.Executing++
		m.running++
		m.saturated = true
		s.handlerDoneLocked(r, Status{Code: code}, duration)
	}
}

func TestUnmetTargetOriginalAIMDCharacterization(t *testing.T) {
	s, sample := controlFixture(t, 0)
	for i := 0; i < 80; i++ {
		sample(OK, 20*time.Millisecond, false)
	}
	m := s.Governance().Methods[0]
	if m.Limit != 2 || m.Mode != ControlAdaptive || m.Fallbacks != 0 {
		t.Fatal(m)
	}
	t.Log("original controller stays at Min=2 for 20ms service / 5ms target; synthetic characterization, not performance evidence")
}

func TestAdaptiveGuardLatchesAndManualResetFencesOldSamples(t *testing.T) {
	s, sample := controlFixture(t, 4)
	for i := 0; i < 20; i++ {
		sample(OK, 20*time.Millisecond, false)
	}
	if m := s.Governance().Methods[0]; m.Limit != 2 || m.UnmetWindows != 1 || m.Mode != ControlAdaptive {
		t.Fatal(m)
	}
	for i := 0; i < 4; i++ {
		sample(OK, 20*time.Millisecond, false)
	}
	if m := s.Governance().Methods[0]; m.Limit != 4 || m.Mode != ControlFallback || m.Fallbacks != 1 {
		t.Fatal(m)
	}
	for i := 0; i < 40; i++ {
		sample(OK, time.Millisecond, false)
	}
	if m := s.Governance().Methods[0]; m.Limit != 4 || m.Mode != ControlFallback || m.Fallbacks != 1 {
		t.Fatal("fallback automatically oscillated", m)
	}
	before := s.Governance().Methods[0].Samples
	require(t, s.ResetAdaptive("/demo/echo"))
	for i := 0; i < 20; i++ {
		sample(OK, 20*time.Millisecond, true)
	}
	m := s.Governance().Methods[0]
	if m.Limit != 8 || m.Mode != ControlAdaptive || m.Generation != 1 || m.Resets != 1 || m.Samples != before || m.UnmetWindows != 0 {
		t.Fatal("stale generation sampled after reset", m)
	}
	for i := 0; i < 4; i++ {
		sample(OK, time.Millisecond, false)
	}
	if m := s.Governance().Methods[0]; m.Samples != before+4 || m.Limit != 8 {
		t.Fatal(m)
	}
}

func TestAdaptiveGuardNeverRaisesLimitFromFailureWindows(t *testing.T) {
	for _, code := range []Code{Unavailable, ResourceExhausted, DeadlineExceeded, Internal, InvalidArgument} {
		t.Run(string(code), func(t *testing.T) {
			s, sample := controlFixture(t, 4)
			for i := 0; i < 20; i++ {
				sample(OK, 20*time.Millisecond, false)
			}
			sample(code, 20*time.Millisecond, false)
			count := 3
			if code == Internal || code == InvalidArgument {
				count = 4
			}
			for i := 0; i < count; i++ {
				sample(OK, 20*time.Millisecond, false)
			}
			if m := s.Governance().Methods[0]; m.Mode != ControlAdaptive || m.Limit != 2 || m.UnmetWindows != 0 {
				t.Fatal("failure window raised capacity", m)
			}
			for i := 0; i < 8; i++ {
				sample(OK, 20*time.Millisecond, false)
			}
			if m := s.Governance().Methods[0]; m.Mode != ControlFallback || m.Limit != 4 {
				t.Fatal(m)
			}
		})
	}
}

func TestAdaptiveGuardCancellationNeutralAndReachableTarget(t *testing.T) {
	s, sample := controlFixture(t, 4)
	for i := 0; i < 20; i++ {
		sample(OK, 20*time.Millisecond, false)
	}
	before := s.Governance().Methods[0].Samples
	for i := 0; i < 16; i++ {
		sample(Canceled, time.Second, false)
	}
	if m := s.Governance().Methods[0]; m.Samples != before || m.UnmetWindows != 1 {
		t.Fatal(m)
	}
	for i := 0; i < 4; i++ {
		sample(OK, time.Millisecond, false)
	}
	if m := s.Governance().Methods[0]; m.UnmetWindows != 0 || m.Mode != ControlAdaptive || m.Limit != 3 {
		t.Fatal(m)
	}
}

func TestAdaptiveGuardPolicyValidationAndCopy(t *testing.T) {
	for _, policy := range []MethodPolicy{
		{FallbackLimit: -1}, {FallbackLimit: 9}, {FallbackLimit: 1}, {FallbackWindows: 2}, {FallbackLimit: 4, FallbackWindows: -1}, {FallbackLimit: 4, FallbackWindows: 65},
	} {
		policy.Method = "/demo/echo"
		policy.Min = 2
		policy.Initial = 8
		policy.Max = 8
		if _, err := NewServer(ServerConfig{Governance: &GovernanceConfig{MaxExecuting: 8, Adaptive: true, Methods: []MethodPolicy{policy}}}); err == nil {
			t.Fatal("invalid policy accepted", policy)
		}
	}
	cfg := &GovernanceConfig{MaxExecuting: 8, Adaptive: true, Methods: []MethodPolicy{{Method: "/demo/echo", FallbackLimit: 4}}}
	s, err := NewServer(ServerConfig{Governance: cfg})
	require(t, err)
	cfg.Methods[0].FallbackLimit = 7
	echo(s)
	if p := s.governor.methods["/demo/echo"].policy; p.FallbackLimit != 4 || p.FallbackWindows != 3 {
		t.Fatal(p)
	}
	cfg.Adaptive = false
	if _, err = NewServer(ServerConfig{Governance: cfg}); err == nil {
		t.Fatal("guard enabled for static controller")
	}
}

func TestResetAdaptiveBoundsAndLifecycle(t *testing.T) {
	s, _ := controlFixture(t, 4)
	if err := s.ResetAdaptive("/demo/missing"); err == nil {
		t.Fatal("unknown method reset")
	}
	for i := 0; i < 200; i++ {
		require(t, s.ResetAdaptive("/demo/echo"))
	}
	g := s.Governance()
	if len(g.Decisions) != 128 || g.DroppedDecisions != 72 || g.Methods[0].Generation != 200 || g.Methods[0].Resets != 200 {
		t.Fatal(g)
	}
	s.mu.Lock()
	s.state = draining
	s.mu.Unlock()
	if err := s.ResetAdaptive("/demo/echo"); !errors.Is(err, ErrDraining) {
		t.Fatal(err)
	}
	s.Close()
	if err := s.ResetAdaptive("/demo/echo"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	static, err := NewServer(ServerConfig{})
	require(t, err)
	if err = static.ResetAdaptive("/demo/echo"); err == nil {
		t.Fatal("reset without adaptive governor")
	}
}

func TestResetAdaptiveKeepsRunningCallsAndConcurrentSnapshots(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "tcp", true: "quic"}[native], func(t *testing.T) {
			started, release := make(chan struct{}), make(chan struct{})
			var calls atomic.Int64
			register := func(s *Server) {
				require(t, RegisterUnary(s, "/demo/echo", func(ctx context.Context, n int) (int, error) {
					calls.Add(1)
					if n == 0 {
						close(started)
						select {
						case <-release:
						case <-ctx.Done():
							return 0, ctx.Err()
						}
					}
					return n, nil
				}))
			}
			cfg := ServerConfig{Governance: &GovernanceConfig{Adaptive: true, Feedback: true, SampleWindow: 4, Methods: []MethodPolicy{{Method: "/demo/echo", Min: 2, Initial: 8, Max: 8, FallbackLimit: 4}}}}
			var s *Server
			var caller Caller
			if native {
				var c *QUICClient
				s, c, _ = startQUIC(t, cfg, QUICConfig{}, register, ClientConfig{Feedback: &FeedbackConfig{}})
				caller = c
			} else {
				var address string
				s, address = start(t, cfg, register)
				caller = connect(t, address, ClientConfig{Feedback: &FeedbackConfig{}})
			}
			done := make(chan error, 1)
			go func() { var out int; done <- caller.Call(context.Background(), "/demo/echo", 0, &out) }()
			signal(t, started)
			var wg sync.WaitGroup
			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for j := 0; j < 20; j++ {
						if err := s.ResetAdaptive("/demo/echo"); err != nil {
							t.Error(err)
						}
						_ = s.Governance()
					}
				}()
			}
			wg.Wait()
			if m := s.Governance().Methods[0]; m.Running != 1 {
				t.Fatal("reset retired a running handler", m)
			}
			close(release)
			require(t, <-done)
			eventually(t, func() bool { return s.Stats().InFlight == 0 })
			if m := s.Governance().Methods[0]; m.Samples != 0 || m.Generation != 160 {
				t.Fatal("old call polluted new generation", m)
			}
			var out int
			require(t, caller.Call(context.Background(), "/demo/echo", 7, &out))
			eventually(t, func() bool { return s.Stats().InFlight == 0 && s.Stats().NativeStreams == 0 })
			if out != 7 || calls.Load() != 2 || s.Governance().Methods[0].Samples != 1 || s.MemoryStats().Receive.Used != 0 {
				t.Fatal(out, calls.Load(), s.Governance(), s.MemoryStats())
			}
		})
	}
}

func FuzzAdaptiveGuard(f *testing.F) {
	f.Add([]byte{0, 0, 0, 0, 0, 0, 0, 0, 4, 5, 6})
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 1024 {
			return
		}
		s, sample := controlFixture(t, 4)
		for _, op := range operations {
			switch op % 7 {
			case 0:
				sample(OK, 20*time.Millisecond, false)
			case 1:
				sample(OK, time.Millisecond, false)
			case 2:
				sample(ResourceExhausted, 20*time.Millisecond, false)
			case 3:
				sample(Canceled, time.Second, false)
			case 4:
				require(t, s.ResetAdaptive("/demo/echo"))
			case 5:
				sample(OK, 20*time.Millisecond, true)
			case 6:
				sample(Internal, 20*time.Millisecond, false)
			}
			state := s.Governance()
			m := state.Methods[0]
			if m.Limit < 2 || m.Limit > 8 || m.Mode == ControlFallback && m.Limit != 4 || len(state.Decisions) > 128 || m.UnmetWindows > 2 || m.Resets != m.Generation || state.Executing != 0 {
				t.Fatal(state)
			}
		}
	})
}

func TestAdaptiveFallbackPressureAndResetOnRealTransports(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "tcp", true: "quic"}[native], func(t *testing.T) {
			var calls atomic.Int64
			register := func(s *Server) {
				require(t, RegisterUnary(s, "/demo/echo", func(ctx context.Context, n int) (int, error) {
					calls.Add(1)
					timer := time.NewTimer(3 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-timer.C:
						return n, nil
					case <-ctx.Done():
						return 0, ctx.Err()
					}
				}))
			}
			cfg := ServerConfig{Governance: &GovernanceConfig{Adaptive: true, Feedback: true, MaxExecuting: 8, SampleWindow: 4, Methods: []MethodPolicy{{Method: "/demo/echo", Min: 2, Initial: 2, Max: 8, Target: time.Millisecond, FallbackLimit: 4, FallbackWindows: 2}}}}
			var s *Server
			var caller Caller
			var feedback func() FeedbackStats
			if native {
				var c *QUICClient
				s, c, _ = startQUIC(t, cfg, QUICConfig{}, register, ClientConfig{Feedback: &FeedbackConfig{}})
				caller = c
				feedback = c.FeedbackStats
			} else {
				var address string
				s, address = start(t, cfg, register)
				c := connect(t, address, ClientConfig{Feedback: &FeedbackConfig{}})
				caller = c
				feedback = c.FeedbackStats
			}
			for n := 0; n < 8; n++ {
				var out int
				require(t, caller.Call(context.Background(), "/demo/echo", n, &out))
				if out != n {
					t.Fatal(out)
				}
			}
			if m := s.Governance().Methods[0]; m.Mode != ControlFallback || m.Limit != 4 || m.Fallbacks != 1 {
				t.Fatal(m)
			}
			if f := feedback(); len(f.Methods) != 1 || f.Methods[0].Recommended != 4 || f.Methods[0].Active != 0 {
				t.Fatal(f)
			}
			require(t, s.ResetAdaptive("/demo/echo"))
			if m := s.Governance().Methods[0]; m.Limit != 2 || m.Mode != ControlAdaptive || m.Generation != 1 {
				t.Fatal(m)
			}
			var out int
			require(t, caller.Call(context.Background(), "/demo/echo", 42, &out))
			if f := feedback(); f.Methods[0].Recommended != 2 || f.Methods[0].Active != 0 {
				t.Fatal(f)
			}
			eventually(t, func() bool { return s.Stats().InFlight == 0 && s.Stats().NativeStreams == 0 })
			if calls.Load() != 9 || out != 42 || s.MemoryStats().Receive.Used != 0 {
				t.Fatal(calls.Load(), out, s.Stats(), s.MemoryStats())
			}
		})
	}
}
