package kanarpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

// These reproductions also compile on R9B, without the additive R9C APIs.
func outcome(t testing.TB, s *Server, name string) uint64 {
	t.Helper()
	b, err := json.Marshal(s.Stats())
	require(t, err)
	var fields map[string]json.RawMessage
	require(t, json.Unmarshal(b, &fields))
	var values map[string]uint64
	if fields["Outcomes"] == nil {
		t.Fatalf("missing lifecycle observations: %s", b)
	}
	require(t, json.Unmarshal(fields["Outcomes"], &values))
	n, ok := values[name]
	if !ok {
		t.Fatalf("missing lifecycle observation %s: %s", name, b)
	}
	return n
}

func TestBoundaryBudgetRejectionHasLocalProvenance(t *testing.T) {
	r := newResources(MemoryConfig{ReceiveBytes: 1024, SendBytes: 1024, ControlBytes: 1024, EncodingBytes: 1024})
	_, err := r.send.lease(1025)
	var status *Status
	if !errors.As(err, &status) || status.Code != ResourceExhausted {
		t.Fatal(err)
	}
	if reflect.TypeOf(err) == reflect.TypeOf(&Status{}) {
		t.Fatal("framework quota refusal indistinguishable from business ResourceExhausted", err)
	}
	if statusOf(err).Code != ResourceExhausted || r.snapshot().Send.Rejected != 1 {
		t.Fatal("lost wire compatibility or cumulative refusal", err)
	}
}

func TestBoundaryCancelAfterTerminalIsObserved(t *testing.T) {
	s, addr := start(t, ServerConfig{AckTimeout: time.Second}, echo)
	conn, err := net.Dial("tcp", addr)
	require(t, err)
	defer conn.Close()
	rawOpen(t, conn, 1, "/demo/echo", false, 1)
	f, _ := rawTerminal(t, conn)
	require(t, wire.Write(conn, wire.Frame{Kind: wire.Cancel, ID: f.ID}, wire.HardLimit))
	eventually(t, func() bool { return s.Stats().Completed == 1 })
	if got := outcome(t, s, "CancelRequests"); got != 1 {
		t.Fatal("late explicit CANCEL disappeared", got, s.Stats())
	}
	if got := outcome(t, s, "ReceiptsCanceled"); got != 1 || s.Stats().LostTerminals != 0 {
		t.Fatal("explicit cancel misclassified as missing receipt", got, s.Stats())
	}
}

func TestBoundaryDrainPreservesDeliveryFaultAlongsideCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	s, addr := start(t, ServerConfig{AckTimeout: 30 * time.Millisecond}, func(s *Server) {
		echo(s)
		require(t, RegisterUnary(s, "/demo/hold", func(context.Context, int) (int, error) {
			close(started)
			<-release
			return 1, nil
		}))
	})
	conn, err := net.Dial("tcp", addr)
	require(t, err)
	defer conn.Close()
	rawOpen(t, conn, 1, "/demo/echo", false, 1)
	rawTerminal(t, conn) // Deliberately never acknowledge this real terminal.
	eventually(t, func() bool { return s.Stats().LostTerminals == 1 && s.Stats().Completed == 1 })
	rawOpen(t, conn, 2, "/demo/hold", false, 1)
	signal(t, started)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = s.Drain(ctx)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrDelivery) {
		t.Fatal("drain hid one of the two independent failures", err, s.Stats())
	}
	if s.Stats().LostTerminals != 1 || s.Stats().Executing != 1 {
		t.Fatal("history cleared or uncooperative work hidden", s.Stats())
	}
}
