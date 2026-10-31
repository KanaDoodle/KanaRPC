package kanarpc

import (
	"errors"
	"sort"
	"sync"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

// MemoryPoolConfig limits aggregate framework buffers. DataBytes covers encoding,
// receive and send together. Control receive/send each retain a separate bound.
// Zero values use 32 MiB data, 16 MiB per data class and 64 KiB per control class.
type MemoryPoolConfig struct {
	Memory    MemoryConfig
	DataBytes int64
}

type BufferUsage struct {
	Streams, Chunks int
	Bytes           int64
}
type BufferStats struct {
	Streams, Chunks         int
	Bytes                   int64
	PeakStreams, PeakChunks int
	PeakBytes               int64
}
type MemoryUsage struct{ Receive, Send, ControlReceive, ControlSend, Encoding int64 }
type MemoryGroupStats struct {
	Accounts, Leases, EncodingWaiters int
	Bytes                             MemoryUsage
	DataBytes                         int64
	Unread                            BufferUsage // Current group occupancy; peaks are reported at pool/account level.
}
type MemoryPoolStats struct {
	Memory                        MemoryStats
	Data, Total                   ByteStats
	Active, Retired               MemoryGroupStats
	Unread                        BufferStats
	Accounts, PeakAccounts, Limit int
	Created, Released             uint64
	Sealed                        bool
}
type MemoryAccountStats struct {
	ID                                    uint64
	Retired, Bound, OwnerClosed, Released bool
	Memory                                MemoryStats
	DataBytes, TotalBytes                 int64
	Leases, EncodingWaiters               int
	Unread                                BufferStats
}

// MemoryPool owns only accounting, never sockets, request tables or user buffers.
// All attached local and aggregate budgets use this single mutex. No pool method
// calls into a connection or routing, which keeps the lock order one-way.
type MemoryPool struct {
	mu                sync.Mutex
	cfg               MemoryPoolConfig
	classes           [5]ByteStats
	data, total       ByteStats
	changed           chan struct{}
	accounts          map[*MemoryAccount]struct{}
	limit, peak       int
	created, released uint64
	sealed            bool
	unread            BufferStats
}
type MemoryAccount struct {
	pool                                  *MemoryPool
	id                                    uint64
	retired, bound, ownerClosed, released bool
	resources                             *resources
	leases, waiters                       int
	unread                                BufferStats
}
type memoryKind uint8

const (
	receiveMemory memoryKind = iota
	sendMemory
	controlReceiveMemory
	controlSendMemory
	encodingMemory
)

func (k memoryKind) data() bool { return k == receiveMemory || k == sendMemory || k == encodingMemory }

func NewMemoryPool(cfg MemoryPoolConfig, maxAccounts int) (*MemoryPool, error) {
	var err error
	if cfg.Memory, err = memoryConfig(cfg.Memory); err != nil {
		return nil, err
	}
	if cfg.DataBytes == 0 {
		cfg.DataBytes = 32 << 20
	}
	if maxAccounts < 1 || maxAccounts > 4096 || cfg.DataBytes < 1024 || cfg.DataBytes > 1<<40 {
		return nil, errors.New("kanarpc: invalid pool bounds")
	}
	p := &MemoryPool{cfg: cfg, limit: maxAccounts, accounts: make(map[*MemoryAccount]struct{})}
	p.classes = [5]ByteStats{{Limit: cfg.Memory.ReceiveBytes}, {Limit: cfg.Memory.SendBytes}, {Limit: cfg.Memory.ControlBytes}, {Limit: cfg.Memory.ControlBytes}, {Limit: cfg.Memory.EncodingBytes}}
	p.data.Limit = cfg.DataBytes
	p.total.Limit = cfg.DataBytes + 2*cfg.Memory.ControlBytes
	return p, nil
}

// CheckClient validates capacity for a maximum prepared OPEN plus its send lease.
// It does not reserve an account or bind it to a client.
func (p *MemoryPool) CheckClient(cfg ClientConfig) error {
	cfg.MemoryAccount = nil
	c, err := normalizeClient(cfg)
	if err != nil {
		return err
	}
	if p.cfg.Memory.EncodingBytes < 2*int64(c.IO.MaxFrameBytes) || p.cfg.DataBytes < 3*int64(c.IO.MaxFrameBytes)+wire.HeaderSize {
		return errors.New("kanarpc: pool cannot hold maximum encoding and send leases")
	}
	return nil
}
func (p *MemoryPool) NewAccount() (*MemoryAccount, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sealed {
		return nil, ErrClosed
	}
	if len(p.accounts) >= p.limit {
		return nil, &Status{Code: ResourceExhausted, Message: "memory accounts"}
	}
	p.created++
	a := &MemoryAccount{pool: p, id: p.created}
	p.accounts[a] = struct{}{}
	if len(p.accounts) > p.peak {
		p.peak = len(p.accounts)
	}
	return a, nil
}
func (p *MemoryPool) Seal()         { p.mu.Lock(); p.sealed = true; p.mu.Unlock() }
func (a *MemoryAccount) ID() uint64 { return a.id }
func (a *MemoryAccount) Retire()    { a.pool.mu.Lock(); a.retired = true; a.pool.mu.Unlock() }
func (a *MemoryAccount) bind(r *resources) error {
	p := a.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if a.released || a.bound {
		return errors.New("kanarpc: memory account already bound or released")
	}
	a.bound = true
	a.resources = r
	for k, b := range []*byteBudget{&r.receive, &r.send, &r.controlReceive, &r.controlSend, &r.encoding} {
		b.account = a
		b.kind = memoryKind(k)
	}
	r.buffers.account = a
	return nil
}
func (r *resources) ownerClosed() {
	if a := r.encoding.account; a != nil {
		a.pool.mu.Lock()
		a.ownerClosed = true
		a.pool.notifyLocked()
		a.pool.mu.Unlock()
	}
}

// Release removes a quiescent account. Closing a socket alone cannot forgive
// leases held by a user codec or an unread stream. Retire never frees bytes.
func (a *MemoryAccount) Release() error {
	p := a.pool
	p.mu.Lock()
	defer p.mu.Unlock()
	if a.released {
		return nil
	}
	if a.bound && !a.ownerClosed || a.leases != 0 || a.waiters != 0 || a.unread.Chunks != 0 {
		return errors.New("kanarpc: memory account still owned")
	}
	a.released = true
	delete(p.accounts, a)
	p.released++
	p.notifyLocked()
	return nil
}
func (p *MemoryPool) notifyLocked() {
	if p.changed != nil {
		close(p.changed)
		p.changed = nil
	}
}
func (a *MemoryAccount) snapshotLocked() MemoryAccountStats {
	s := MemoryAccountStats{ID: a.id, Retired: a.retired, Bound: a.bound, OwnerClosed: a.ownerClosed, Released: a.released, Leases: a.leases, EncodingWaiters: a.waiters, Unread: a.unread}
	if r := a.resources; r != nil {
		s.Memory = r.snapshotLocked()
		s.DataBytes = s.Memory.Receive.Used + s.Memory.Send.Used + s.Memory.Encoding.Used
		s.TotalBytes = s.DataBytes + s.Memory.ControlReceive.Used + s.Memory.ControlSend.Used
	}
	return s
}
func (a *MemoryAccount) Snapshot() MemoryAccountStats {
	a.pool.mu.Lock()
	defer a.pool.mu.Unlock()
	return a.snapshotLocked()
}
func (p *MemoryPool) Accounts() []MemoryAccountStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]MemoryAccountStats, 0, len(p.accounts))
	for a := range p.accounts {
		out = append(out, a.snapshotLocked())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
func (p *MemoryPool) Stats() MemoryPoolStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	s := MemoryPoolStats{Memory: MemoryStats{p.classes[0], p.classes[1], p.classes[2], p.classes[3], p.classes[4]}, Data: p.data, Total: p.total, Unread: p.unread, Accounts: len(p.accounts), PeakAccounts: p.peak, Limit: p.limit, Created: p.created, Released: p.released, Sealed: p.sealed}
	for a := range p.accounts {
		v := a.snapshotLocked()
		g := &s.Active
		if a.retired {
			g = &s.Retired
		}
		g.Accounts++
		g.Leases += v.Leases
		g.EncodingWaiters += v.EncodingWaiters
		g.DataBytes += v.DataBytes
		g.Bytes.Receive += v.Memory.Receive.Used
		g.Bytes.Send += v.Memory.Send.Used
		g.Bytes.Encoding += v.Memory.Encoding.Used
		g.Bytes.ControlReceive += v.Memory.ControlReceive.Used
		g.Bytes.ControlSend += v.Memory.ControlSend.Used
		g.Unread.Streams += v.Unread.Streams
		g.Unread.Chunks += v.Unread.Chunks
		g.Unread.Bytes += v.Unread.Bytes
	}
	return s
}
func (s *BufferStats) change(streams, chunks int, bytes int64) {
	s.Streams += streams
	s.Chunks += chunks
	s.Bytes += bytes
	if s.Streams > s.PeakStreams {
		s.PeakStreams = s.Streams
	}
	if s.Chunks > s.PeakChunks {
		s.PeakChunks = s.Chunks
	}
	if s.Bytes > s.PeakBytes {
		s.PeakBytes = s.Bytes
	}
}
