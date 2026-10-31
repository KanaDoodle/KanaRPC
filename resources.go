package kanarpc

import (
	"context"
	"errors"
	"sync"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

// MemoryConfig bounds retained framework record buffers, not arbitrary
// application allocations, decoded response objects, QUIC internals or RSS.
type MemoryConfig struct{ ReceiveBytes, SendBytes, ControlBytes, EncodingBytes int64 }
type ByteStats struct {
	Limit, Used, Peak int64
	Rejected          uint64
}
type MemoryStats struct{ Receive, Send, ControlReceive, ControlSend, Encoding ByteStats }

// BudgetError identifies a local framework reservation refusal. Across the
// wire only Status is transmitted: a remote ResourceExhausted or reset does
// not establish which budget (or whether a budget) refused the peer.
// Scope is owner, pool_class, pool_data, or closed_owner. Used is the usage
// used for this decision (zero for an impossible absolute-size reservation).
type BudgetError struct {
	Class, Scope           string
	Requested, Used, Limit int64
	status                 Status
}

func (e *BudgetError) Error() string { return e.status.Error() }
func (e *BudgetError) Unwrap() error { return &e.status }

func (b *byteBudget) refusalLocked(n int64, absolute bool, message string) error {
	e := &BudgetError{Class: [...]string{"receive", "send", "control_receive", "control_send", "encoding"}[b.kind], Scope: "owner", Requested: n, Used: b.stats.Used, Limit: b.stats.Limit, status: Status{Code: ResourceExhausted, Message: message}}
	if absolute {
		e.Used = 0
	}
	if n < 0 || n > e.Limit-e.Used || b.account == nil {
		return e
	}
	a := b.account
	if a.released || a.ownerClosed {
		e.Scope = "closed_owner"
		return e
	}
	e.Scope, e.Used, e.Limit = "pool_class", a.pool.classes[b.kind].Used, a.pool.classes[b.kind].Limit
	if absolute {
		e.Used = 0
	}
	if n > e.Limit-e.Used || !b.kind.data() {
		return e
	}
	e.Scope, e.Used, e.Limit = "pool_data", a.pool.data.Used, a.pool.data.Limit
	if absolute {
		e.Used = 0
	}
	return e
}

type byteBudget struct {
	mu      sync.Mutex
	stats   ByteStats
	changed chan struct{}
	account *MemoryAccount
	kind    memoryKind
}

type byteLease struct {
	budget   *byteBudget
	held     int64
	released bool
}

// Attached budgets share the pool mutex, making both levels one reservation.
func (b *byteBudget) lock() {
	if b.account != nil {
		b.account.pool.mu.Lock()
	} else {
		b.mu.Lock()
	}
}
func (b *byteBudget) unlock() {
	if b.account != nil {
		b.account.pool.mu.Unlock()
	} else {
		b.mu.Unlock()
	}
}
func (b *byteBudget) capacityLocked(n int64, absolute bool) bool {
	used := b.stats.Used
	if absolute {
		used = 0
	}
	if n < 0 || n > b.stats.Limit-used {
		return false
	}
	if a := b.account; a != nil {
		p := a.pool
		if a.released || a.ownerClosed {
			return false
		}
		used = p.classes[b.kind].Used
		if absolute {
			used = 0
		}
		if n > p.classes[b.kind].Limit-used {
			return false
		}
		if b.kind.data() {
			used = p.data.Used
			if absolute {
				used = 0
			}
			if n > p.data.Limit-used {
				return false
			}
		}
	}
	return true
}
func addBytes(s *ByteStats, n int64) {
	s.Used += n
	if s.Used > s.Peak {
		s.Peak = s.Used
	}
}
func (b *byteBudget) changeLocked(n int64) {
	addBytes(&b.stats, n)
	if a := b.account; a != nil {
		p := a.pool
		addBytes(&p.classes[b.kind], n)
		if b.kind.data() {
			addBytes(&p.data, n)
		}
		addBytes(&p.total, n)
	}
}
func (b *byteBudget) rejectedLocked(n int64) {
	b.stats.Rejected++
	if a := b.account; a != nil {
		p := a.pool
		p.classes[b.kind].Rejected++
		if b.kind.data() && n > p.data.Limit-p.data.Used {
			p.data.Rejected++
		}
		p.total.Rejected++
	}
}
func (b *byteBudget) leaseLocked(n int64) *byteLease {
	b.changeLocked(n)
	if a := b.account; a != nil {
		a.leases++
	}
	return &byteLease{budget: b, held: n}
}
func (b *byteBudget) lease(n int64) (*byteLease, error) {
	b.lock()
	defer b.unlock()
	if !b.capacityLocked(n, false) {
		b.rejectedLocked(n)
		return nil, b.refusalLocked(n, false, "record byte budget")
	}
	return b.leaseLocked(n), nil
}
func (b *byteBudget) waitLease(ctx context.Context, n int64) (*byteLease, error) {
	for {
		if err := deadlineError(ctx); err != nil {
			return nil, err
		}
		b.lock()
		if !b.capacityLocked(n, true) {
			b.rejectedLocked(n)
			err := b.refusalLocked(n, true, "encoding capacity")
			b.unlock()
			return nil, err
		}
		if b.capacityLocked(n, false) {
			l := b.leaseLocked(n)
			b.unlock()
			return l, nil
		}
		var changed <-chan struct{}
		if a := b.account; a != nil {
			p := a.pool
			if p.changed == nil {
				p.changed = make(chan struct{})
			}
			changed = p.changed
			a.waiters++
		} else {
			if b.changed == nil {
				b.changed = make(chan struct{})
			}
			changed = b.changed
		}
		b.unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		if b.account != nil {
			b.lock()
			b.account.waiters--
			b.unlock()
		}
	}
}
func (b *byteBudget) notifyLocked() {
	if b.account != nil {
		b.account.pool.notifyLocked()
		return
	}
	if b.changed != nil {
		close(b.changed)
		b.changed = nil
	}
}
func (b *byteBudget) take(n int64) (func(), error) {
	l, err := b.lease(n)
	if err != nil {
		return nil, err
	}
	return l.release, nil
}
func (l *byteLease) release() {
	b := l.budget
	b.lock()
	defer b.unlock()
	if l.released {
		return
	}
	l.released = true
	b.changeLocked(-l.held)
	l.held = 0
	if a := b.account; a != nil {
		a.leases--
	}
	b.notifyLocked()
}
func (l *byteLease) shrink(n int64) bool {
	b := l.budget
	b.lock()
	defer b.unlock()
	if l.released || n < 0 || n > l.held {
		return false
	}
	b.changeLocked(n - l.held)
	l.held = n
	b.notifyLocked()
	return true
}
func (b *byteBudget) snapshot() ByteStats { b.lock(); defer b.unlock(); return b.stats }
func memoryConfig(c MemoryConfig) (MemoryConfig, error) {
	if c.ReceiveBytes == 0 {
		c.ReceiveBytes = 16 << 20
	}
	if c.SendBytes == 0 {
		c.SendBytes = 16 << 20
	}
	if c.ControlBytes == 0 {
		c.ControlBytes = 64 << 10
	}
	if c.EncodingBytes == 0 {
		c.EncodingBytes = 16 << 20
	}
	if c.ReceiveBytes < 1024 || c.SendBytes < 1024 || c.ControlBytes < 4096 || c.EncodingBytes < 1024 || c.EncodingBytes > 1<<40 || c.ReceiveBytes > 1<<40 || c.SendBytes > 1<<40 || c.ControlBytes > 16<<20 {
		return c, errors.New("kanarpc: invalid record byte bounds")
	}
	return c, nil
}

type resources struct {
	compression                                compressionCounters
	feedback                                   *feedbackTracker
	admission                                  *clientAdmission
	receive, send, controlReceive, controlSend byteBudget
	encoding                                   byteBudget
	buffers                                    bufferOwner
}

func newResources(c MemoryConfig) *resources {
	return &resources{receive: byteBudget{kind: receiveMemory, stats: ByteStats{Limit: c.ReceiveBytes}}, send: byteBudget{kind: sendMemory, stats: ByteStats{Limit: c.SendBytes}}, controlReceive: byteBudget{kind: controlReceiveMemory, stats: ByteStats{Limit: c.ControlBytes}}, controlSend: byteBudget{kind: controlSendMemory, stats: ByteStats{Limit: c.ControlBytes}}, encoding: byteBudget{kind: encodingMemory, stats: ByteStats{Limit: c.EncodingBytes}}, buffers: bufferOwner{entries: make(map[*pending]struct{})}}
}
func (r *resources) snapshotLocked() MemoryStats {
	return MemoryStats{r.receive.stats, r.send.stats, r.controlReceive.stats, r.controlSend.stats, r.encoding.stats}
}
func (r *resources) snapshot() MemoryStats {
	if a := r.encoding.account; a != nil {
		a.pool.mu.Lock()
		defer a.pool.mu.Unlock()
		return r.snapshotLocked()
	}
	return MemoryStats{r.receive.snapshot(), r.send.snapshot(), r.controlReceive.snapshot(), r.controlSend.snapshot(), r.encoding.snapshot()}
}
func newClientResources(c ClientConfig) (*resources, error) {
	r := newResources(c.Memory)
	r.feedback = newFeedback(c.Feedback, c.MaxInFlight)
	r.admission = newAdmission(c.MaxInFlight)
	if c.MemoryAccount != nil {
		if err := c.MemoryAccount.bind(r); err != nil {
			return nil, err
		}
	}
	return r, nil
}
func dataKind(kind wire.Kind) bool {
	return kind == wire.Open || kind == wire.OpenEncoded || kind == wire.OpenOptions || kind == wire.Data || kind == wire.DataEncoded || kind == wire.DataOptions
}
func (r *resources) reserveRead(kind wire.Kind, n uint32) (func(), error) {
	if !dataKind(kind) {
		if n > 4096 {
			return nil, ErrProtocol
		}
		return r.controlReceive.take(int64(n))
	}
	size := int64(n)
	if kind == wire.Open || kind == wire.OpenEncoded || kind == wire.OpenOptions {
		size *= 2
	} // Frame plus request/metadata copies; shared by both payload codecs.
	return r.receive.take(size)
}
func (r *resources) reserveWrite(kind wire.Kind, n int) (func(), error) {
	if !dataKind(kind) {
		if n > 4096 {
			return nil, ErrProtocol
		}
		return r.controlSend.take(int64(n) + wire.HeaderSize)
	}
	return r.send.take(int64(n) + wire.HeaderSize)
}
func (c *Client) MemoryStats() MemoryStats     { return c.resources.snapshot() }
func (c *QUICClient) MemoryStats() MemoryStats { return c.resources.snapshot() }
func (s *Server) MemoryStats() MemoryStats     { return s.resources.snapshot() }

// Completed unread streams remain charged until consumed, explicitly closed,
// or their public parent client closes. A QUIC child IO cleanup preserves them.
type bufferOwner struct {
	mu      sync.Mutex
	entries map[*pending]struct{}
	stats   BufferStats
	account *MemoryAccount
}

// Callers hold pending.mu before bufferOwner.mu. Pool snapshots never take
// either lock. Unread is a subset of receive, never a second byte charge.
func (b *bufferOwner) changeLocked(streams, chunks int, bytes int64) {
	b.stats.change(streams, chunks, bytes)
	if a := b.account; a != nil {
		a.pool.mu.Lock()
		a.unread.change(streams, chunks, bytes)
		a.pool.unread.change(streams, chunks, bytes)
		a.pool.mu.Unlock()
	}
}
func (b *bufferOwner) add(p *pending, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	streams := 0
	if _, ok := b.entries[p]; !ok {
		b.entries[p] = struct{}{}
		streams = 1
	}
	b.changeLocked(streams, 1, int64(n))
}
func (b *bufferOwner) consume(p *pending, n int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	streams := 0
	if len(p.queue) == 0 {
		delete(b.entries, p)
		streams = -1
	}
	b.changeLocked(streams, -1, -int64(n))
}
func (b *bufferOwner) drop(p *pending) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.entries[p]; !ok {
		return
	}
	delete(b.entries, p)
	var bytes int64
	for _, chunk := range p.queue {
		bytes += int64(len(chunk.body))
	}
	b.changeLocked(-1, -len(p.queue), -bytes)
}
func (b *bufferOwner) snapshot() BufferStats   { b.mu.Lock(); defer b.mu.Unlock(); return b.stats }
func (c *Client) BufferStats() BufferStats     { return c.resources.buffers.snapshot() }
func (c *QUICClient) BufferStats() BufferStats { return c.resources.buffers.snapshot() }
func (b *bufferOwner) clear() {
	b.mu.Lock()
	all := make([]*pending, 0, len(b.entries))
	for p := range b.entries {
		all = append(all, p)
	}
	b.mu.Unlock()
	for _, p := range all {
		p.mu.Lock()
		p.dropLocked(ErrClosed)
		p.mu.Unlock()
	}
}

type receivedChunk struct {
	body    []byte
	release func()
}

func (p *pending) dropLocked(err error) {
	p.client.resources.buffers.drop(p)
	for _, chunk := range p.queue {
		chunk.release()
	}
	p.queue = nil
	p.discard = true
	p.err = err
	p.notifyLocked()
}
