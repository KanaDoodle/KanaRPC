package kanarpc

import (
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func TestNativePartialOpenClaimsRespectSharedByteBudget(t *testing.T) {
	s, c, _ := startQUIC(t, ServerConfig{Memory: MemoryConfig{ReceiveBytes: 2 << 20}}, QUICConfig{}, echo)
	first, err := c.conn.OpenStreamSync(context.Background())
	require(t, err)
	defer first.CancelWrite(nativeCancel)
	defer first.CancelRead(nativeCancel)
	header := make([]byte, wire.HeaderSize)
	copy(header, "KNR2")
	header[4] = 2
	header[5] = byte(wire.Open)
	binary.BigEndian.PutUint64(header[8:16], 1)
	binary.BigEndian.PutUint32(header[16:20], 1<<20)
	_, err = first.Write(header)
	require(t, err)
	eventually(t, func() bool { return s.MemoryStats().Receive.Used == 2<<20 })
	for i := 0; i < 3; i++ {
		stream, e := c.conn.OpenStreamSync(context.Background())
		require(t, e)
		_ = stream.SetDeadline(time.Now().Add(time.Second))
		_, e = stream.Write(header)
		require(t, e)
		var b [1]byte
		if _, e = stream.Read(b[:]); e == nil {
			t.Fatal("over-budget frame accepted")
		}
		stream.CancelRead(nativeCancel)
		stream.CancelWrite(nativeCancel)
	}
	stats := s.MemoryStats().Receive
	if stats.Peak > stats.Limit || stats.Rejected < 3 || s.Stats().Executing != 0 {
		t.Fatal(stats, s.Stats())
	}
	first.CancelWrite(nativeCancel)
	first.CancelRead(nativeCancel)
	eventually(t, func() bool { return s.MemoryStats().Receive.Used == 0 })
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 42, &out))
	if out != 42 {
		t.Fatal(out)
	}
}
func TestCompletedUnreadStreamsStayChargedAndCloseReleasesBytes(t *testing.T) {
	for _, native := range []bool{false, true} {
		t.Run(map[bool]string{false: "tcp", true: "quic"}[native], func(t *testing.T) {
			register := func(s *Server) {
				require(t, RegisterStream(s, "/demo/one", func(_ context.Context, _ int, send func(string) error) error { return send(strings.Repeat("x", 4096)) }))
			}
			var caller Caller
			var memory func() MemoryStats
			var pending func() bool
			if native {
				s, c, _ := startQUIC(t, ServerConfig{}, QUICConfig{}, register)
				_ = s
				caller = c
				memory = c.MemoryStats
				pending = func() bool { return c.Stats().Active == 0 }
			} else {
				_, address := start(t, ServerConfig{}, register)
				c := connect(t, address, ClientConfig{})
				caller = c
				memory = c.MemoryStats
				pending = func() bool { return c.Stats().Pending == 0 }
			}
			stream, err := caller.Stream(context.Background(), "/demo/one", 0)
			require(t, err)
			eventually(t, pending)
			if memory().Receive.Used < 4096 {
				t.Fatal("completed buffers escaped budget", memory())
			}
			require(t, stream.Close())
			eventually(t, func() bool { return memory().Receive.Used == 0 })
			stream, err = caller.Stream(context.Background(), "/demo/one", 0)
			require(t, err)
			eventually(t, pending)
			var value string
			require(t, stream.Recv(&value))
			if len(value) != 4096 {
				t.Fatal(len(value))
			}
			if memory().Receive.Used != 0 {
				t.Fatal(memory())
			}
			stream, err = caller.Stream(context.Background(), "/demo/one", 0)
			require(t, err)
			eventually(t, pending)
			caller.Close()
			if memory().Receive.Used != 0 {
				t.Fatal("parent close retained completed buffers", memory())
			}
		})
	}
}
func TestByteSaturationKeepsCancelAndTerminalControlWritable(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	s, c, _ := startQUIC(t, ServerConfig{Memory: MemoryConfig{SendBytes: 1024}}, QUICConfig{}, func(s *Server) {
		echo(s)
		require(t, RegisterStream(s, "/demo/hold", func(ctx context.Context, _ int, send func(string) error) error {
			close(started)
			defer close(stopped)
			if e := send(strings.Repeat("x", 2048)); e != nil {
				return e
			}
			<-ctx.Done()
			return ctx.Err()
		}))
	})
	stream, err := c.Stream(context.Background(), "/demo/hold", 0)
	require(t, err)
	signal(t, started)
	var value string
	var status *Status
	if err := stream.Recv(&value); !errors.As(err, &status) || status.Code != ResourceExhausted {
		t.Fatal(err)
	}
	signal(t, stopped)
	var out int
	require(t, c.Call(context.Background(), "/demo/echo", 9, &out))
	eventually(t, func() bool {
		m := s.MemoryStats()
		return m.Send.Used == 0 && m.Receive.Used == 0 && m.ControlSend.Used == 0 && m.ControlReceive.Used == 0
	})
	if s.MemoryStats().Send.Peak > s.MemoryStats().Send.Limit {
		t.Fatal(s.MemoryStats())
	}
}
func TestByteReservationsReleaseOnceUnderRace(t *testing.T) {
	budget := &byteBudget{stats: ByteStats{Limit: 1024}}
	release, err := budget.take(1024)
	require(t, err)
	if _, err = budget.take(1); err == nil {
		t.Fatal("budget overcommitted")
	}
	var group sync.WaitGroup
	for i := 0; i < 32; i++ {
		group.Add(1)
		go func() { defer group.Done(); release() }()
	}
	group.Wait()
	if state := budget.snapshot(); state.Used != 0 || state.Peak != 1024 || state.Rejected != 1 {
		t.Fatal(state)
	}
}
func FuzzByteBudget(f *testing.F) {
	f.Add([]byte{1, 4, 0, 255})
	f.Fuzz(func(t *testing.T, operations []byte) {
		if len(operations) > 4096 {
			return
		}
		budget := &byteBudget{stats: ByteStats{Limit: 512}}
		var releases []func()
		for _, op := range operations {
			if op%3 == 0 && len(releases) > 0 {
				releases[len(releases)-1]()
				releases = releases[:len(releases)-1]
			} else {
				if release, err := budget.take(int64(op)); err == nil {
					releases = append(releases, release)
				}
			}
			state := budget.snapshot()
			if state.Used < 0 || state.Used > state.Limit {
				t.Fatal(state)
			}
		}
		for _, release := range releases {
			release()
			release()
		}
		if budget.snapshot().Used != 0 {
			t.Fatal(budget.snapshot())
		}
	})
}
