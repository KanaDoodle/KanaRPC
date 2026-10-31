package kanarpc

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func poolFixture(t testing.TB, count int) (*MemoryPool, []*resources, []*MemoryAccount) {
	t.Helper()
	p, err := NewMemoryPool(MemoryPoolConfig{DataBytes: 4096, Memory: MemoryConfig{ReceiveBytes: 1024, SendBytes: 1024, EncodingBytes: 2048}}, count)
	if err != nil {
		t.Fatal(err)
	}
	var rs []*resources
	var as []*MemoryAccount
	for i := 0; i < count; i++ {
		a, e := p.NewAccount()
		if e != nil {
			t.Fatal(e)
		}
		cfg, e := normalizeClient(ClientConfig{IO: IOConfig{MaxFrameBytes: 1024}, MemoryAccount: a})
		if e != nil {
			t.Fatal(e)
		}
		r, e := newClientResources(cfg)
		if e != nil {
			t.Fatal(e)
		}
		rs = append(rs, r)
		as = append(as, a)
	}
	t.Cleanup(func() {
		for i, a := range as {
			rs[i].ownerClosed()
			if err := a.Release(); err != nil {
				t.Errorf("account %d: %v %+v", i, err, a.Snapshot())
			}
		}
		if s := p.Stats(); s.Total.Used != 0 || s.Accounts != 0 {
			t.Errorf("leaked pool: %+v", s)
		}
	})
	return p, rs, as
}
func assertPool(t testing.TB, p *MemoryPool) {
	t.Helper()
	s := p.Stats()
	m := s.Memory
	sum := m.Receive.Used + m.Send.Used + m.Encoding.Used
	if s.Data.Used != sum || s.Total.Used != sum+m.ControlReceive.Used+m.ControlSend.Used || s.Active.DataBytes+s.Retired.DataBytes != sum {
		t.Fatalf("accounting mismatch %+v", s)
	}
	for _, b := range []ByteStats{m.Receive, m.Send, m.Encoding, m.ControlReceive, m.ControlSend, s.Data, s.Total} {
		if b.Used < 0 || b.Used > b.Limit || b.Peak > b.Limit {
			t.Fatalf("overcommit %+v", s)
		}
	}
	if s.Unread.Bytes > m.Receive.Used || s.Accounts > s.Limit {
		t.Fatalf("ownership mismatch %+v", s)
	}
}
func TestPoolAtomicLocalClassAndTotalAdmission(t *testing.T) {
	p, rs, as := poolFixture(t, 2)
	first, err := rs[0].encoding.lease(2048)
	require(t, err)
	defer first.release()
	if _, err = rs[1].encoding.lease(1); err == nil {
		t.Fatal("cross-connection encoding overcommit")
	}
	if rs[1].encoding.snapshot().Used != 0 {
		t.Fatal("partial reservation")
	}
	require(t, func() error {
		if err := as[0].Release(); err == nil {
			return errors.New("live owner released")
		}
		return nil
	}())
	as[0].Retire()
	s := p.Stats()
	if s.Retired.DataBytes != 2048 || s.Active.DataBytes != 0 || s.Retired.Leases != 1 {
		t.Fatal(s)
	}
	send, err := rs[1].send.lease(1024)
	require(t, err)
	defer send.release()
	recv, err := rs[1].receive.lease(1024)
	require(t, err)
	defer recv.release()
	if _, err = rs[0].receive.lease(1); err == nil {
		t.Fatal("data total overcommit")
	}
	control, err := rs[0].controlSend.lease(4096)
	require(t, err)
	control.release()
	assertPool(t, p)
	if !first.shrink(1000) {
		t.Fatal("shrink failed")
	}
	next, err := rs[1].encoding.lease(1048)
	require(t, err)
	next.release()
	assertPool(t, p)
}
func TestPoolWaitWakesAcrossAccountsAndCancels(t *testing.T) {
	p, rs, _ := poolFixture(t, 2)
	l, err := rs[0].encoding.lease(2048)
	require(t, err)
	defer l.release()
	done := make(chan *byteLease, 1)
	go func() {
		l, e := rs[1].encoding.waitLease(context.Background(), 2048)
		if e != nil {
			done <- nil
			return
		}
		done <- l
	}()
	eventually(t, func() bool { return p.Stats().Active.EncodingWaiters == 1 })
	if rs[1].encoding.snapshot().Used != 0 {
		t.Fatal("waiter holds partial lease")
	}
	l.release()
	next := <-done
	if next == nil {
		t.Fatal("wait failed")
	}
	defer next.release()
	ctx, cancel := context.WithCancel(context.Background())
	waited := make(chan error, 1)
	go func() { _, e := rs[0].encoding.waitLease(ctx, 2048); waited <- e }()
	eventually(t, func() bool { return p.Stats().Active.EncodingWaiters == 1 })
	cancel()
	if e := <-waited; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	assertPool(t, p)
}
func TestPoolDuplicateZeroLeasesAndRetiredSnapshots(t *testing.T) {
	p, rs, as := poolFixture(t, 2)
	l, err := rs[0].controlReceive.lease(0)
	require(t, err)
	if p.Stats().Active.Leases != 1 {
		t.Fatal(p.Stats())
	}
	as[0].Retire()
	rs[0].ownerClosed()
	if as[0].Release() == nil {
		t.Fatal("zero lease lost")
	}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); l.release(); l.shrink(0); _ = p.Stats(); _ = p.Accounts() }()
	}
	wg.Wait()
	if l.shrink(0) {
		t.Fatal("released lease shrunk")
	}
	require(t, as[0].Release())
	if p.Stats().Retired.Leases != 0 || p.Stats().Released != 1 {
		t.Fatal(p.Stats())
	}
	p.Seal()
	if _, e := p.NewAccount(); !errors.Is(e, ErrClosed) {
		t.Fatal(e)
	}
	assertPool(t, p)
}
func TestPoolSendFailureIsUntouchedAndReleases(t *testing.T) {
	p, rs, _ := poolFixture(t, 2)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	cfg, err := normalizeIO(IOConfig{MaxFrameBytes: 1024})
	require(t, err)
	first := newLink(a, cfg)
	first.resources = rs[0]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, e := first.send(ctx, wire.Frame{Kind: wire.Data, ID: 1, Number: 1, Payload: make([]byte, 700)})
		done <- e
	}()
	eventually(t, func() bool { return p.Stats().Memory.Send.Used == 724 })
	c, d := net.Pipe()
	defer c.Close()
	defer d.Close()
	second := newLink(c, cfg)
	second.resources = rs[1]
	touched, e := second.send(context.Background(), wire.Frame{Kind: wire.Data, ID: 1, Number: 1, Payload: make([]byte, 700)})
	if touched || e == nil {
		t.Fatal(touched, e)
	}
	if rs[1].send.snapshot().Used != 0 {
		t.Fatal("failed send retained local bytes")
	}
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal(e)
	}
	if p.Stats().Memory.Send.Used != 0 {
		t.Fatal(p.Stats())
	}
	assertPool(t, p)
}
func TestPoolPartialReceiveClaimsBeforePayloadAndRollsBack(t *testing.T) {
	p, rs, _ := poolFixture(t, 2)
	cfg, err := normalizeIO(IOConfig{MaxFrameBytes: 1024})
	require(t, err)
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	first := newLink(a, cfg)
	first.resources = rs[0]
	header := make([]byte, wire.HeaderSize)
	copy(header, "KNR2")
	header[4] = 2
	header[5] = byte(wire.Data)
	binary.BigEndian.PutUint64(header[8:16], 1)
	binary.BigEndian.PutUint32(header[16:20], 700)
	binary.BigEndian.PutUint32(header[20:24], 1)
	done := make(chan error, 1)
	go func() { _, e := first.read(); done <- e }()
	_, err = b.Write(header)
	require(t, err)
	eventually(t, func() bool { return p.Stats().Memory.Receive.Used == 700 })
	c, d := net.Pipe()
	defer c.Close()
	defer d.Close()
	second := newLink(c, cfg)
	second.resources = rs[1]
	refused := make(chan error, 1)
	go func() { _, e := second.read(); refused <- e }()
	_, err = d.Write(header)
	require(t, err)
	select {
	case err = <-refused:
		if err == nil {
			t.Fatal("claim accepted")
		}
	case <-time.After(time.Second):
		t.Fatal("read waited for unadmitted payload")
	}
	if rs[1].receive.snapshot().Used != 0 {
		t.Fatal("partial local receive claim")
	}
	b.Close()
	if <-done == nil {
		t.Fatal("partial frame accepted")
	}
	assertPool(t, p)
	if p.Stats().Total.Used != 0 {
		t.Fatal(p.Stats())
	}
}
func TestPoolValidationBindingAndAccountBound(t *testing.T) {
	p, err := NewMemoryPool(MemoryPoolConfig{}, 1)
	require(t, err)
	a, err := p.NewAccount()
	require(t, err)
	if _, err = p.NewAccount(); err == nil {
		t.Fatal("unbounded accounts")
	}
	require(t, p.CheckClient(ClientConfig{}))
	if p.CheckClient(ClientConfig{IO: IOConfig{MaxFrameBytes: 16 << 20}}) == nil {
		t.Fatal("impossible encoding allowed")
	}
	for _, cfg := range []MemoryPoolConfig{{DataBytes: 3072}, {DataBytes: 8192, Memory: MemoryConfig{EncodingBytes: 1024}}} {
		small, e := NewMemoryPool(cfg, 1)
		require(t, e)
		if small.CheckClient(ClientConfig{IO: IOConfig{MaxFrameBytes: 1024}}) == nil {
			t.Fatal("invalid shared headroom accepted", cfg)
		}
	}
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	c, err := NewClient(left, ClientConfig{MemoryAccount: a})
	require(t, err)
	defer c.Close()
	x, y := net.Pipe()
	defer x.Close()
	defer y.Close()
	if _, err = NewClient(x, ClientConfig{MemoryAccount: a}); err == nil {
		t.Fatal("account rebound")
	}
	c.Close()
	require(t, a.Release())
	if _, err = NewClient(x, ClientConfig{MemoryAccount: a}); err == nil {
		t.Fatal("released account rebound")
	}
	if p.Stats().Created != 1 || p.Stats().Released != 1 {
		t.Fatal(p.Stats())
	}
}
func FuzzSharedByteLeases(f *testing.F) {
	f.Add([]byte{255, 1, 2, 11, 12, 33, 67, 0})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 2048 {
			return
		}
		p, rs, as := poolFixture(t, 2)
		var leases []*byteLease
		for i, op := range ops {
			r := rs[i%2]
			budgets := []*byteBudget{&r.receive, &r.send, &r.encoding, &r.controlSend, &r.controlReceive}
			switch op % 5 {
			case 0:
				if len(leases) > 0 {
					leases[len(leases)-1].release()
					leases[len(leases)-1].release()
					leases = leases[:len(leases)-1]
				}
			case 1:
				if len(leases) > 0 {
					leases[len(leases)-1].shrink(int64(op))
				}
			case 2:
				as[i%2].Retire()
			default:
				l, e := budgets[int(op)%5].lease(int64(op) * 7)
				if e == nil {
					leases = append(leases, l)
				}
			}
			assertPool(t, p)
		}
		for _, l := range leases {
			l.release()
		}
		assertPool(t, p)
	})
}
func BenchmarkByteLeaseAccounting(b *testing.B) {
	for _, shared := range []bool{false, true} {
		name := "local"
		if shared {
			name = "shared"
		}
		b.Run(name, func(b *testing.B) {
			var budget *byteBudget
			if shared {
				_, rs, _ := poolFixture(b, 1)
				budget = &rs[0].send
			} else {
				budget = &byteBudget{stats: ByteStats{Limit: 1 << 20}}
			}
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					l, e := budget.lease(24)
					if e != nil {
						b.Fatal(e)
					}
					l.release()
				}
			})
		})
	}
}

func TestPoolControlRecordWritableWhileDataFull(t *testing.T) {
	p, rs, _ := poolFixture(t, 2)
	encoding, e := rs[0].encoding.lease(2048)
	require(t, e)
	defer encoding.release()
	send, e := rs[0].send.lease(1024)
	require(t, e)
	defer send.release()
	receive, e := rs[0].receive.lease(1024)
	require(t, e)
	defer receive.release()
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	cfg, e := normalizeIO(IOConfig{})
	require(t, e)
	link := newLink(a, cfg)
	link.resources = rs[1]
	done := make(chan error, 1)
	go func() {
		f, e := wire.ReadReserved(b, cfg.MaxFrameBytes, rs[1].reserveRead)
		if e == nil {
			f.Release()
			if f.Kind != wire.Cancel {
				e = ErrProtocol
			}
		}
		done <- e
	}()
	touched, e := link.send(context.Background(), wire.Frame{Kind: wire.Cancel, ID: 1})
	require(t, e)
	if !touched {
		t.Fatal("control not written")
	}
	require(t, <-done)
	s := p.Stats()
	if s.Data.Used != 4096 || s.Memory.ControlSend.Peak != 24 || s.Memory.ControlSend.Used != 0 || s.Active.Leases != 3 {
		t.Fatal(s)
	}
	assertPool(t, p)
}
