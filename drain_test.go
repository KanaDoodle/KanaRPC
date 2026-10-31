package kanarpc

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func budgetRefusal(t testing.TB, err error, class, scope string) *BudgetError {
	t.Helper()
	var e *BudgetError
	if !errors.As(err, &e) || e.Class != class || e.Scope != scope || statusOf(err).Code != ResourceExhausted {
		t.Fatalf("wrong local provenance: %#v (%v), expected %s/%s", e, err, class, scope)
	}
	return e
}

func TestBudgetProvenanceClassesAndPoolLayers(t *testing.T) {
	r := newResources(MemoryConfig{1024, 1024, 1024, 1024})
	for name, b := range map[string]*byteBudget{"receive": &r.receive, "send": &r.send, "encoding": &r.encoding, "control_receive": &r.controlReceive, "control_send": &r.controlSend} {
		t.Run(name, func(t *testing.T) {
			_, err := b.lease(1025)
			e := budgetRefusal(t, err, name, "owner")
			if e.Requested != 1025 || e.Limit != 1024 || e.Used != 0 || b.snapshot().Rejected != 1 {
				t.Fatal(e, b.snapshot())
			}
		})
	}
	_, err := r.encoding.waitLease(context.Background(), 1025)
	budgetRefusal(t, err, "encoding", "owner")
	var e *BudgetError
	if errors.As(&Status{Code: ResourceExhausted, Message: "record byte budget"}, &e) {
		t.Fatal("business status acquired local quota provenance")
	}
	p, rs, _ := poolFixture(t, 2)
	encoding, err := rs[0].encoding.lease(2048)
	require(t, err)
	defer encoding.release()
	_, err = rs[1].encoding.lease(1)
	budgetRefusal(t, err, "encoding", "pool_class")
	send, err := rs[1].send.lease(1024)
	require(t, err)
	defer send.release()
	recv, err := rs[1].receive.lease(1024)
	require(t, err)
	defer recv.release()
	_, err = rs[0].receive.lease(1)
	budgetRefusal(t, err, "receive", "pool_class") // First blocking gate wins.
	assertPool(t, p)
	// Closing an otherwise empty owner is a refusal, not a global exhaustion.
	r2 := newResources(MemoryConfig{1024, 1024, 1024, 2048})
	p2, err := NewMemoryPool(MemoryPoolConfig{DataBytes: 1024}, 1)
	require(t, err)
	a, err := p2.NewAccount()
	require(t, err)
	require(t, a.bind(r2))
	hold, err := r2.send.lease(1024)
	require(t, err)
	_, err = r2.receive.lease(1)
	budgetRefusal(t, err, "receive", "pool_data")
	hold.release()
	r2.ownerClosed()
	_, err = r2.send.lease(1)
	budgetRefusal(t, err, "send", "closed_owner")
	require(t, a.Release())
}

// Runs real sockets and native QUIC streams, while allowing deliberate ACK/FIN
// omissions that the public client correctly prevents in normal operation.
func boundaryTransport(t testing.TB, kind Transport, cfg ServerConfig, register func(*Server)) (*Server, recordChannel) {
	t.Helper()
	if kind == QUIC {
		s, c, _ := startQUIC(t, cfg, QUICConfig{}, register)
		stream, err := c.conn.OpenStreamSync(context.Background())
		require(t, err)
		channel := &nativeChannel{Stream: stream}
		require(t, channel.SetDeadline(time.Now().Add(3*time.Second)))
		t.Cleanup(func() { _ = channel.Close() })
		return s, channel
	}
	if kind == TCP {
		s, addr := start(t, cfg, register)
		c, err := net.Dial("tcp", addr)
		require(t, err)
		require(t, c.SetDeadline(time.Now().Add(3*time.Second)))
		t.Cleanup(func() { _ = c.Close() })
		return s, c
	}
	credentials, err := testcert.New()
	require(t, err)
	s, err := NewServer(cfg)
	require(t, err)
	register(s)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require(t, err)
	done := make(chan error, 1)
	go func() { done <- s.ServeTLS(l, TLSOptions{Config: credentials.Server}) }()
	eventually(t, s.Ready)
	t.Cleanup(func() {
		s.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		require(t, s.Wait(ctx))
		require(t, <-done)
	})
	security := credentials.Client.Clone()
	security.NextProtos = []string{tlsALPN}
	c, err := tls.Dial("tcp", l.Addr().String(), security)
	require(t, err)
	require(t, c.SetDeadline(time.Now().Add(3*time.Second)))
	t.Cleanup(func() { _ = c.Close() })
	return s, c
}

func boundaryOpen(t testing.TB, c io.Writer, id uint64, path string, streaming bool) {
	t.Helper()
	b, err := json.Marshal(openRecord{Method: path, BudgetNanos: int64(2 * time.Second), Window: 1, Stream: streaming, Request: json.RawMessage("0")})
	require(t, err)
	require(t, wire.Write(c, wire.Frame{Kind: wire.Open, ID: id, Payload: b}, wire.HardLimit))
}

func boundaryEnd(t testing.TB, c io.Reader) (wire.Frame, endRecord) {
	t.Helper()
	for {
		f, err := wire.Read(c, wire.HardLimit)
		require(t, err)
		if f.Kind == wire.End {
			var end endRecord
			require(t, json.Unmarshal(f.Payload, &end))
			return f, end
		}
	}
}

func boundaryACK(t testing.TB, c io.Writer, f wire.Frame, end endRecord) {
	t.Helper()
	b, err := hex.DecodeString(end.Receipt)
	require(t, err)
	if len(b) != 16 {
		t.Fatal("expected receipt", end)
	}
	require(t, wire.Write(c, wire.Frame{Kind: wire.Ack, ID: f.ID, Payload: b}, wire.HardLimit))
}

func assertReceipts(t testing.TB, s *Server) {
	t.Helper()
	v := s.Stats()
	if o := v.Outcomes; o.ReceiptsResolved != v.Delivered+v.LostTerminals+o.ReceiptsCanceled+o.ReceiptsDeadlineExceeded {
		t.Fatal("receipt accounting", v)
	}
}

func TestDrainBoundaryPhysicalTransports(t *testing.T) {
	for _, kind := range []Transport{TCP, TLS, QUIC} {
		for _, behavior := range []string{"ack", "cancel", "deadline", "no-ack", "peer-abort", "ack-no-fin", "budget", "business-status"} {
			t.Run(string(kind)+"/"+behavior, func(t *testing.T) {
				cfg := ServerConfig{AckTimeout: 150 * time.Millisecond}
				if behavior == "deadline" {
					cfg.MaxRequestDuration = 50 * time.Millisecond
				}
				if behavior == "budget" {
					cfg.Memory.SendBytes = 1024
				}
				s, c := boundaryTransport(t, kind, cfg, func(s *Server) {
					require(t, s.HandleStream("/demo/items", func(_ context.Context, _ json.RawMessage, out *Sender) error {
						if behavior == "business-status" {
							return &Status{Code: ResourceExhausted, Message: "record byte budget"}
						}
						return out.Send(json.RawMessage(`"` + strings.Repeat("x", 1500) + `"`))
					}))
				})
				boundaryOpen(t, c, 1, "/demo/items", true)
				f, end := boundaryEnd(t, c)
				if behavior == "budget" || behavior == "business-status" {
					if end.Code != ResourceExhausted {
						t.Fatal(end)
					}
				}
				switch behavior {
				case "ack", "budget", "business-status", "ack-no-fin":
					boundaryACK(t, c, f, end)
					if kind == QUIC && behavior != "ack-no-fin" {
						require(t, c.(*nativeChannel).Stream.Close())
					}
				case "cancel":
					require(t, wire.Write(c, wire.Frame{Kind: wire.Cancel, ID: f.ID}, wire.HardLimit))
				case "peer-abort":
					require(t, c.Close())
				}
				eventually(t, func() bool { return s.Stats().Completed == 1 })
				stats := s.Stats()
				o := stats.Outcomes
				assertReceipts(t, s)
				lost := behavior == "no-ack" || behavior == "peer-abort"
				incomplete := behavior == "ack-no-fin" && kind == QUIC
				if (stats.LostTerminals == 1) != lost || (stats.IncompleteExchanges == 1) != incomplete {
					t.Fatal("wrong delivery boundary", stats)
				}
				if (o.CancelRequests == 1) != (behavior == "cancel") || (o.ReceiptsCanceled == 1) != (behavior == "cancel") || (o.RequestDeadlines == 1) != (behavior == "deadline") || (o.ReceiptsDeadlineExceeded == 1) != (behavior == "deadline") {
					t.Fatal("status mistaken for protocol event", stats)
				}
				if (s.MemoryStats().Send.Rejected == 1) != (behavior == "budget") || stats.Rejected != 0 {
					t.Fatal("byte refusal mistaken for admission or business refusal", s.MemoryStats(), stats)
				}
				if behavior == "peer-abort" && o.TransportAborts != 1 {
					t.Fatal("peer reset hidden", stats)
				}
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				report, err := s.DrainWithReport(ctx)
				if errors.Is(err, ErrDelivery) != (lost || incomplete) || !report.Quiescent || report.AddedFaults != (DeliveryFaults{}) || report.PriorFaults != report.TotalFaults {
					t.Fatal("drain obscured historical fault or resource completion", report, err)
				}
			})
		}
	}
}

func TestDrainReportFaultDuringDrainAndRepeatedConcurrentDrain(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, c := boundaryTransport(t, TCP, ServerConfig{AckTimeout: 50 * time.Millisecond}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
			close(started)
			select {
			case <-release:
			case <-ctx.Done():
			}
			return n, ctx.Err()
		}))
	})
	boundaryOpen(t, c, 1, "/demo/hold", false)
	signal(t, started)
	type result struct {
		report DrainReport
		err    error
	}
	done := make(chan result, 1)
	go func() { r, err := s.DrainWithReport(context.Background()); done <- result{r, err} }()
	eventually(t, func() bool { return !s.Ready() })
	close(release)
	boundaryEnd(t, c)
	r := <-done
	if !errors.Is(r.err, ErrDelivery) || r.report.PriorFaults.LostTerminals != 0 || r.report.AddedFaults.LostTerminals != 1 || !r.report.Quiescent {
		t.Fatal(r)
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			report, err := s.DrainWithReport(context.Background())
			if err != ErrDelivery || report.PriorFaults.LostTerminals != 1 || report.AddedFaults != (DeliveryFaults{}) || !report.Quiescent {
				t.Error("history reset", report, err)
			}
		}()
	}
	wg.Wait()
}

func TestReceiptSettlementACKPriorityAndIdempotence(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded, io.EOF} {
		s, err := NewServer(ServerConfig{})
		require(t, err)
		ctx, cancel := context.WithCancelCause(context.Background())
		p := &serverConn{server: s, ctx: ctx}
		r := &execution{peer: p, ctx: ctx, ack: make(chan struct{}), terminal: true}
		require(t, func() error {
			if !r.acknowledge(make([]byte, 16)) {
				return ErrProtocol
			}
			return nil
		}())
		cancel(cause)
		var wg sync.WaitGroup
		for i := 0; i < 16; i++ {
			wg.Add(1)
			go func() { defer wg.Done(); r.recordDelivery() }()
		}
		wg.Wait()
		assertReceipts(t, s)
		if v := s.Stats(); v.Delivered != 1 || v.LostTerminals != 0 || v.Outcomes.ReceiptsResolved != 1 {
			t.Fatal(v)
		}
	}
}

func TestTransportDeadlineCauseIsNotRequestDeadlineOrExplicitCancel(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		s, err := NewServer(ServerConfig{})
		require(t, err)
		parent, stop := context.WithCancelCause(context.Background())
		ctx, cancel := context.WithCancelCause(parent)
		defer cancel(nil)
		r := &execution{peer: &serverConn{server: s, ctx: parent}, ctx: ctx, ack: make(chan struct{})}
		stop(cause) // Transport termination is not a received CANCEL or budget expiry.
		r.recordDelivery()
		r.recordStop()
		v := s.Stats()
		if v.LostTerminals != 1 || v.Outcomes.TransportAborts != 1 || v.Outcomes.RequestDeadlines != 0 || v.Outcomes.ReceiptsCanceled != 0 || v.Outcomes.ReceiptsDeadlineExceeded != 0 {
			t.Fatal(v)
		}
	}
}

func TestDuplicateCancelAndQueueCancellationAreSingleObservations(t *testing.T) {
	started, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer close(release)
	s, c := boundaryTransport(t, TCP, ServerConfig{Governance: &GovernanceConfig{MaxExecuting: 1, QueueCapacity: 1}}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
			close(started)
			<-ctx.Done()
			close(canceled)
			<-release
			return n, ctx.Err()
		}))
	})
	boundaryOpen(t, c, 1, "/demo/hold", false)
	signal(t, started)
	boundaryOpen(t, c, 2, "/demo/hold", false)
	eventually(t, func() bool { return s.Stats().Queued == 1 })
	for _, id := range []uint64{2, 2, 1, 1} {
		require(t, wire.Write(c, wire.Frame{Kind: wire.Cancel, ID: id}, wire.HardLimit))
	}
	signal(t, canceled)
	eventually(t, func() bool { return s.Stats().Queued == 0 && s.Stats().Outcomes.CancelRequests == 2 })
	if v := s.Stats(); v.Admitted != 2 || v.Outcomes.CancelRequests != 2 || v.LostTerminals != 0 {
		t.Fatal(v)
	}
}

func TestAdmissionRefusalReasons(t *testing.T) {
	for _, policy := range []bool{false, true} {
		t.Run(map[bool]string{false: "capacity", true: "policy"}[policy], func(t *testing.T) {
			cfg := ServerConfig{MaxInFlight: 1}
			if policy {
				cfg.MaxInFlight = 100
				cfg.Governance = &GovernanceConfig{MaxExecuting: 1, QueueCapacity: 1}
			}
			started, release := make(chan struct{}), make(chan struct{})
			s, c := boundaryTransport(t, TCP, cfg, func(s *Server) {
				require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) {
					select {
					case <-started:
					default:
						close(started)
					}
					select {
					case <-release:
					case <-ctx.Done():
					}
					return n, ctx.Err()
				}))
			})
			defer close(release)
			boundaryOpen(t, c, 1, "/demo/hold", false)
			signal(t, started)
			if policy {
				boundaryOpen(t, c, 2, "/demo/hold", false)
				eventually(t, func() bool { return s.Stats().Queued == 1 })
			}
			boundaryOpen(t, c, 3, "/demo/hold", false)
			f, end := boundaryEnd(t, c)
			if f.ID != 3 || end.Code != ResourceExhausted || end.Receipt != "" {
				t.Fatal(f, end)
			}
			v := s.Stats()
			if v.Rejected != 1 || (v.Outcomes.CapacityRejected == 1) == policy || (v.Outcomes.PolicyRejected == 1) != policy {
				t.Fatal(v)
			}
			if s.MemoryStats().Send.Rejected != 0 {
				t.Fatal("admission refusal forged a byte refusal")
			}
		})
	}
}

func TestDrainReportUncooperativeHandlerRemainsOwned(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	s, c := boundaryTransport(t, TCP, ServerConfig{}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(context.Context, int) (int, error) { close(started); <-release; return 1, nil }))
	})
	boundaryOpen(t, c, 1, "/demo/hold", false)
	signal(t, started)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report, err := s.DrainWithReport(ctx)
	if !errors.Is(err, context.Canceled) || report.Quiescent || report.After.Executing != 1 || report.After.InFlight != 1 {
		close(release)
		t.Fatal(report, err)
	}
	close(release)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	require(t, s.Wait(ctx))
	report, err = s.DrainWithReport(ctx)
	if err != nil || !report.Quiescent || report.After.Outcomes.ShutdownAborts != 1 || report.After.Outcomes.CancelRequests != 0 {
		t.Fatal(report, err)
	}
}

func TestClientReceiveBudgetRefusalDoesNotBecomeRemoteUserCancel(t *testing.T) {
	s, addr := start(t, ServerConfig{}, func(s *Server) {
		require(t, s.HandleUnary("/demo/large", func(context.Context, json.RawMessage) (json.RawMessage, error) {
			return json.RawMessage(`"` + strings.Repeat("x", 1500) + `"`), nil
		}))
	})
	c := connect(t, addr, ClientConfig{Memory: MemoryConfig{ReceiveBytes: 1024}})
	var out string
	err := c.Call(context.Background(), "/demo/large", 0, &out)
	budgetRefusal(t, err, "receive", "owner")
	eventually(t, func() bool { return s.Stats().Completed == 1 })
	v := s.Stats()
	if v.Delivered != 0 || v.LostTerminals != 1 || v.Outcomes.CancelRequests != 0 || v.Outcomes.ReceiptsCanceled != 0 || c.MemoryStats().Receive.Rejected != 1 {
		t.Fatal(v, c.MemoryStats())
	}
}

func TestLocalReceiveBudgetAbortIsNotPeerCancel(t *testing.T) {
	started := make(chan struct{})
	s, c := boundaryTransport(t, TCP, ServerConfig{Memory: MemoryConfig{ReceiveBytes: 1024}}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) { close(started); <-ctx.Done(); return n, ctx.Err() }))
	})
	boundaryOpen(t, c, 1, "/demo/hold", false)
	signal(t, started)
	body := []byte(`{"method":"/demo/hold","budget_ns":1000000000,"window":1,"request":"` + strings.Repeat("x", 600) + `"}`)
	_ = wire.Write(c, wire.Frame{Kind: wire.Open, ID: 2, Payload: body}, wire.HardLimit)
	eventually(t, func() bool { return s.Stats().InFlight == 0 })
	v := s.Stats()
	if v.Outcomes.BudgetAborts != 1 || v.Outcomes.TransportAborts != 0 || v.Outcomes.CancelRequests != 0 || s.MemoryStats().Receive.Rejected != 1 {
		t.Fatal(v, s.MemoryStats())
	}
}

// Fuzz the terminal settlement cutoff against an independent small event model.
// ACK means protocol receipt, not application commit; duplicate/late controls
// must not mint deliveries or erase a previously settled failure.
func FuzzReceiptSettlement(f *testing.F) {
	for _, seed := range [][]byte{{0, 4, 4}, {1, 4, 0}, {2, 4, 0}, {3, 0, 4}, {0, 1, 2, 4}, {4, 0, 1, 4}} {
		f.Add(seed)
	}
	f.Fuzz(func(t *testing.T, events []byte) {
		if len(events) > 64 {
			events = events[:64]
		}
		s, err := NewServer(ServerConfig{})
		require(t, err)
		parent, peerCancel := context.WithCancelCause(context.Background())
		defer peerCancel(nil)
		ctx, cancel := context.WithCancelCause(parent)
		defer cancel(nil)
		r := &execution{peer: &serverConn{server: s, ctx: parent}, ctx: ctx, cancel: cancel, ack: make(chan struct{}), terminal: true}
		acked, settled, cancelSeen := false, false, false
		firstCause := ""
		wantDelivered, wantLost, wantCanceled := uint64(0), uint64(0), uint64(0)
		settle := func() {
			if !settled {
				settled = true
				switch {
				case acked:
					wantDelivered = 1
				case firstCause == "cancel":
					wantCanceled = 1
				default:
					wantLost = 1
				}
			}
			r.recordDelivery()
		}
		for _, event := range events {
			switch event % 5 {
			case 0:
				if !r.acknowledge(make([]byte, 16)) {
					t.Fatal("valid receipt rejected")
				}
				if !settled {
					acked = true
				}
			case 1:
				r.cancelRequest()
				cancelSeen = true
				if firstCause == "" {
					firstCause = "cancel"
				}
			case 2:
				peerCancel(io.EOF)
				if firstCause == "" {
					firstCause = "peer"
				}
			case 3:
				if r.acknowledge([]byte("wrong-receipt-16!")) {
					t.Fatal("wrong receipt accepted")
				}
			case 4:
				settle()
			}
		}
		settle()
		assertReceipts(t, s)
		v := s.Stats()
		if v.Delivered != wantDelivered || v.LostTerminals != wantLost || v.Outcomes.ReceiptsCanceled != wantCanceled || v.Outcomes.ReceiptsResolved != 1 || (v.Outcomes.CancelRequests == 1) != cancelSeen {
			t.Fatal(events, v, wantDelivered, wantLost, wantCanceled)
		}
	})
}

func TestDeliveryFaultSurvivesLaterSuccessfulRPC(t *testing.T) {
	s, c := boundaryTransport(t, TCP, ServerConfig{AckTimeout: 30 * time.Millisecond}, echo)
	boundaryOpen(t, c, 1, "/demo/echo", false)
	boundaryEnd(t, c)
	eventually(t, func() bool { return s.Stats().Completed == 1 })
	boundaryOpen(t, c, 2, "/demo/echo", false)
	f, end := boundaryEnd(t, c)
	boundaryACK(t, c, f, end)
	eventually(t, func() bool { return s.Stats().Completed == 2 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	report, err := s.DrainWithReport(ctx)
	if err != ErrDelivery || report.PriorFaults.LostTerminals != 1 || report.AddedFaults.LostTerminals != 0 || report.After.Delivered != 1 || !report.Quiescent {
		t.Fatal(report, err)
	}
	assertReceipts(t, s)
}

func TestRejectControlBudgetAbortRetainsLocalCause(t *testing.T) {
	started := make(chan struct{})
	s, c := boundaryTransport(t, TCP, ServerConfig{Memory: MemoryConfig{ControlBytes: 8192}}, func(s *Server) {
		require(t, RegisterUnary(s, "/demo/hold", func(ctx context.Context, n int) (int, error) { close(started); <-ctx.Done(); return n, ctx.Err() }))
	})
	hold, err := s.resources.controlSend.lease(8192)
	require(t, err)
	defer hold.release()
	boundaryOpen(t, c, 1, "/demo/hold", false)
	signal(t, started)
	boundaryOpen(t, c, 2, "/demo/absent", false)
	eventually(t, func() bool { return s.Stats().InFlight == 0 })
	if v := s.Stats(); v.Outcomes.BudgetAborts != 1 || v.Outcomes.TransportAborts != 0 || v.Outcomes.NotFoundRejected != 1 || s.MemoryStats().ControlSend.Rejected != 1 {
		t.Fatal("rejection write erased local budget cause", v, s.MemoryStats())
	}
}
