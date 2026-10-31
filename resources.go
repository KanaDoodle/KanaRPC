package kanarpc

import (
	"errors"
	"sync"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

// MemoryConfig bounds retained framework record buffers, not arbitrary
// application allocations, decoded response objects, QUIC internals or RSS.
type MemoryConfig struct{ ReceiveBytes, SendBytes, ControlBytes int64 }
type ByteStats struct {
	Limit, Used, Peak int64
	Rejected          uint64
}
type MemoryStats struct{ Receive, Send, ControlReceive, ControlSend ByteStats }
type byteBudget struct {
	mu    sync.Mutex
	stats ByteStats
}

func (b *byteBudget) take(n int64) (func(), error) {
	b.mu.Lock()
	if n < 0 || n > b.stats.Limit-b.stats.Used {
		b.stats.Rejected++
		b.mu.Unlock()
		return nil, &Status{Code: ResourceExhausted, Message: "record byte budget"}
	}
	b.stats.Used += n
	if b.stats.Used > b.stats.Peak {
		b.stats.Peak = b.stats.Used
	}
	b.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { b.mu.Lock(); b.stats.Used -= n; b.mu.Unlock() }) }, nil
}
func (b *byteBudget) snapshot() ByteStats { b.mu.Lock(); defer b.mu.Unlock(); return b.stats }
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
	if c.ReceiveBytes < 1024 || c.SendBytes < 1024 || c.ControlBytes < 4096 || c.ReceiveBytes > 1<<40 || c.SendBytes > 1<<40 || c.ControlBytes > 16<<20 {
		return c, errors.New("kanarpc: invalid record byte bounds")
	}
	return c, nil
}

type resources struct {
	feedback                                   *feedbackTracker
	receive, send, controlReceive, controlSend byteBudget
	buffers                                    bufferOwner
}

func newResources(c MemoryConfig) *resources {
	return &resources{receive: byteBudget{stats: ByteStats{Limit: c.ReceiveBytes}}, send: byteBudget{stats: ByteStats{Limit: c.SendBytes}}, controlReceive: byteBudget{stats: ByteStats{Limit: c.ControlBytes}}, controlSend: byteBudget{stats: ByteStats{Limit: c.ControlBytes}}, buffers: bufferOwner{entries: make(map[*pending]struct{})}}
}
func (r *resources) snapshot() MemoryStats {
	return MemoryStats{r.receive.snapshot(), r.send.snapshot(), r.controlReceive.snapshot(), r.controlSend.snapshot()}
}
func dataKind(kind wire.Kind) bool { return kind == wire.Open || kind == wire.Data }
func (r *resources) reserveRead(kind wire.Kind, n uint32) (func(), error) {
	if !dataKind(kind) {
		if n > 4096 {
			return nil, ErrProtocol
		}
		return r.controlReceive.take(int64(n))
	}
	size := int64(n)
	if kind == wire.Open {
		size *= 2
	} // frame plus JSON request/metadata copies
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
}

func (b *bufferOwner) add(p *pending)    { b.mu.Lock(); b.entries[p] = struct{}{}; b.mu.Unlock() }
func (b *bufferOwner) remove(p *pending) { b.mu.Lock(); delete(b.entries, p); b.mu.Unlock() }
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
	for _, chunk := range p.queue {
		chunk.release()
	}
	p.queue = nil
	p.client.resources.buffers.remove(p)
	p.discard = true
	p.err = err
	p.notifyLocked()
}
