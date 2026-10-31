package kanarpc

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

type ClientStats struct {
	Pending, InFlightBufferedChunks          int
	Started, Completed, Canceled, LateFrames uint64
}

// Client owns a single multiplexed byte-stream connection. There is no implicit
// reconnect, business retry, registry worker, or background heartbeat.
type Client struct {
	cfg       ClientConfig
	link      *link
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	closed    bool
	failure   error
	nextID    uint64
	pending   map[uint64]*pending
	stats     ClientStats
	once      sync.Once
	wg        sync.WaitGroup
	single    bool
	resources *resources
}

func Dial(ctx context.Context, address string, cfg ClientConfig) (*Client, error) {
	var err error
	if cfg, err = normalizeClient(cfg); err != nil {
		return nil, err
	}
	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	client, err := NewClient(conn, cfg)
	if err != nil {
		closeSocket(conn)
	}
	return client, err
}

// NewClient transfers conn ownership on success only. It also accepts a
// caller-authenticated TLS connection; TLS construction is not automated here.
func NewClient(conn net.Conn, cfg ClientConfig) (*Client, error) {
	return newRecordClient(conn, cfg, false)
}

func newRecordClient(conn recordChannel, cfg ClientConfig, single bool, shared ...*resources) (*Client, error) {
	if conn == nil {
		return nil, errors.New("kanarpc: nil connection")
	}
	var err error
	if cfg, err = normalizeClient(cfg); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	var res *resources
	if len(shared) > 0 {
		res = shared[0]
	} else {
		res = newClientResources(cfg)
	}
	c := &Client{cfg: cfg, link: newLink(conn, cfg.IO), ctx: ctx, cancel: cancel, pending: make(map[uint64]*pending), single: single, resources: res}
	c.link.resources = res
	c.wg.Add(1)
	go func() { defer c.wg.Done(); c.readLoop() }()
	return c, nil
}

func (c *Client) Stats() ClientStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := c.stats
	stats.Pending = len(c.pending)
	for _, p := range c.pending {
		p.mu.Lock()
		stats.InFlightBufferedChunks += len(p.queue)
		p.mu.Unlock()
	}
	return stats
}
func (c *Client) fail(err error) {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.failure = err
		all := make([]*pending, 0, len(c.pending))
		for _, p := range c.pending {
			all = append(all, p)
		}
		c.mu.Unlock()
		if !c.single {
			c.resources.admission.close()
		}
		// Publish the connection's terminal cause before owner cancellation can
		// race a pending call into a secondary context.Canceled result.
		for _, p := range all {
			p.finish(err, true)
		}
		c.cancel()
		closeSocket(c.link.conn)
		for _, p := range all {
			p.retire()
		}
	})
}

// Close unblocks I/O, cancels local calls, and joins readers/cancel callbacks.
func (c *Client) stopIO() { c.fail(ErrClosed); c.wg.Wait() }
func (c *Client) Close()  { c.fail(ErrClosed); c.resources.buffers.clear(); c.wg.Wait() }

type pending struct {
	client         *Client
	id             uint64
	ctx            context.Context
	cancel         context.CancelFunc
	stop           func() bool
	callbackDone   sync.Once
	retired        sync.Once
	creditMu       sync.Mutex
	mu             sync.Mutex
	changed        chan struct{}
	queue          []receivedChunk
	next           uint32
	window         uint32
	remoteEnded    bool
	openSent       bool
	done, discard  bool
	err            error
	method         string
	leaveFeedback  func()
	attemptedAt    time.Time
	stopOwner      func() bool
	leaveAdmission func()
}

func (p *pending) notifyLocked() { close(p.changed); p.changed = make(chan struct{}) }
func (p *pending) finish(err error, discard bool) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.done {
		return false
	}
	p.done, p.discard, p.err = true, discard, err
	if p.leaveFeedback != nil {
		p.leaveFeedback()
	}
	if discard {
		p.dropLocked(err)
	}
	p.notifyLocked()
	return true
}
func (p *pending) retire() {
	p.retired.Do(func() {
		if p.stop() {
			p.callbackDone.Do(p.client.wg.Done)
		}
		p.cancel()
		if p.stopOwner != nil {
			p.stopOwner()
		}
		c := p.client
		c.mu.Lock()
		delete(c.pending, p.id)
		c.stats.Completed++
		c.mu.Unlock()
		p.leaveFeedback()
		if p.leaveAdmission != nil {
			p.leaveAdmission()
		}
		if c.single {
			finishChannel(c.link.conn)
			c.cancel()
		}
	})
}
func (p *pending) localCancel() {
	defer p.callbackDone.Do(p.client.wg.Done)
	if p.finish(p.ctx.Err(), true) {
		c := p.client
		c.mu.Lock()
		c.stats.Canceled++
		c.mu.Unlock()
		ctx, cancel := context.WithTimeout(c.ctx, c.cfg.ControlTimeout)
		// Open owns the writer permit until its success is recorded. A canceled
		// call that never reached the wire must not emit a future-ID Cancel.
		var touched bool
		err := c.link.acquire(ctx)
		if err == nil {
			p.mu.Lock()
			sent := p.openSent
			p.mu.Unlock()
			if sent {
				touched, err = c.link.writeLocked(ctx, wire.Frame{Kind: wire.Cancel, ID: p.id})
			}
			c.link.release()
		}
		cancel()
		if err != nil && touched {
			c.fail(err)
		}
	}
	p.retire()
}

// Stream begins one server-streaming call. Caller cancellation is always sent
// on the wire. Call and Stream share the same request/terminal state machine.
func (c *Client) Stream(ctx context.Context, method string, request any) (*Stream, error) {
	return c.open(ctx, method, request, true)
}
func (c *Client) open(ctx context.Context, method string, request any, streaming bool) (*Stream, error) {
	prepared, err := prepareRequest(ctx, c.ctx, c.cfg, c.resources, method, request, streaming)
	if err != nil {
		return nil, err
	}
	defer prepared.done()
	return c.openPrepared(prepared)
}
func (c *Client) openPrepared(prepared *preparedCall) (*Stream, error) {
	callCtx := prepared.ctx
	if err := c.link.acquire(callCtx); err != nil {
		if cause := prepared.check(); cause != nil {
			return nil, cause
		}
		return nil, err
	}
	// ID allocation, registration and Open writing share the writer permit, so
	// Open IDs increase on the wire even when callers race to start requests.
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		c.link.release()
		return nil, ErrClosed
	}
	if len(c.pending) >= c.cfg.MaxInFlight {
		c.mu.Unlock()
		c.link.release()
		return nil, &Status{Code: ResourceExhausted, Message: "client in-flight limit"}
	}
	if c.nextID == math.MaxUint64 {
		c.mu.Unlock()
		c.link.release()
		return nil, ErrClosed
	}
	c.nextID++
	prepared.lease.retain()
	prepared.transferred = true
	p := &pending{client: c, id: c.nextID, ctx: callCtx, cancel: prepared.cancel, window: prepared.window, next: 1, changed: make(chan struct{}), method: prepared.method, leaveFeedback: prepared.leaveFeedback, attemptedAt: time.Now(), stopOwner: prepared.stopOwner, leaveAdmission: prepared.lease.release}
	c.wg.Add(1)
	callbackReady := make(chan struct{})
	p.stop = context.AfterFunc(callCtx, func() { <-callbackReady; p.localCancel() })
	c.pending[p.id] = p
	c.stats.Started++
	close(callbackReady)
	c.mu.Unlock()
	b, err := prepared.payload()
	if err != nil {
		c.link.release()
		p.finish(err, true)
		p.retire()
		return nil, err
	}
	touched, err := c.link.writeLocked(callCtx, wire.Frame{Kind: wire.Open, ID: p.id, Payload: b})
	if err == nil {
		p.mu.Lock()
		p.openSent = true
		p.mu.Unlock()
	}
	c.link.release()
	if err != nil {
		c.mu.Lock()
		if c.failure != nil {
			err = c.failure
		}
		c.mu.Unlock()
		p.finish(err, true)
		p.mu.Lock()
		result, discard := p.err, p.discard
		p.mu.Unlock()
		p.retire()
		if touched {
			c.fail(err)
		}
		if !discard {
			return &Stream{pending: p}, nil
		}
		return nil, result
	}
	return &Stream{pending: p}, nil
}

func (c *Client) Call(ctx context.Context, method string, request, response any) error {
	stream, err := c.open(ctx, method, request, false)
	if err != nil {
		return err
	}
	return unaryResult(stream, response)
}
func unaryResult(stream *Stream, response any) error {
	defer stream.Close()
	if err := stream.Recv(response); err != nil {
		if err == io.EOF {
			return ErrProtocol
		}
		return err
	}
	err := stream.Recv(nil)
	if err == io.EOF {
		return nil
	}
	if err == nil {
		return ErrProtocol
	}
	return err
}

func (c *Client) readLoop() {
	for {
		f, err := c.link.read()
		if err != nil {
			c.fail(err)
			return
		}
		if !c.handleFrame(f) {
			return
		}
	}
}
func (c *Client) handleFrame(f wire.Frame) bool {
	retained := false
	defer func() {
		if !retained {
			f.Release()
		}
	}()
	c.mu.Lock()
	p, next := c.pending[f.ID], c.nextID
	if p == nil && f.ID <= next {
		c.stats.LateFrames++
	}
	c.mu.Unlock()
	if f.Kind != wire.Data && f.Kind != wire.End || f.ID > next {
		c.fail(ErrProtocol)
		return false
	}
	if p == nil {
		return true
	}
	if f.Kind == wire.Data {
		p.mu.Lock()
		valid := !p.remoteEnded && f.Number == p.next && len(p.queue) < int(p.window) && json.Valid(f.Payload)
		if valid && !p.done {
			p.next++
			p.queue = append(p.queue, receivedChunk{body: f.Payload, release: f.Release})
			retained = true
			c.resources.buffers.add(p)
			p.notifyLocked()
		}
		canceled := p.done
		p.mu.Unlock()
		if !valid && !canceled {
			c.fail(ErrProtocol)
			return false
		}
		return true
	}
	var end endRecord
	if json.Unmarshal(f.Payload, &end) != nil || !knownCode(end.Code) || len(end.Message) > 256 {
		c.fail(ErrProtocol)
		return false
	}
	c.resources.feedback.observe(p.method, end.Pressure, time.Since(p.attemptedAt))
	var receipt []byte
	var err error
	if end.Receipt != "" {
		receipt, err = hex.DecodeString(end.Receipt)
		if err != nil || len(receipt) != 16 {
			c.fail(ErrProtocol)
			return false
		}
	}
	p.mu.Lock()
	duplicate, canceled := p.remoteEnded, p.done
	p.remoteEnded = true
	p.mu.Unlock()
	if duplicate {
		c.fail(ErrProtocol)
		return false
	}
	p.creditMu.Lock()
	defer p.creditMu.Unlock()
	if len(receipt) != 0 && !canceled {
		ctx, cancel := context.WithTimeout(p.ctx, c.cfg.ControlTimeout)
		touched, e := c.link.send(ctx, wire.Frame{Kind: wire.Ack, ID: p.id, Payload: receipt})
		cancel()
		if e != nil {
			if touched {
				c.fail(e)
				return false
			}
			p.finish(e, true)
			p.retire()
			return true
		}
	}
	if c.single {
		if channel, ok := c.link.conn.(interface{ AwaitEnd(context.Context) error }); ok {
			budget, stop := context.WithTimeout(p.ctx, c.cfg.ControlTimeout)
			e := channel.AwaitEnd(budget)
			stop()
			if e != nil {
				c.fail(e)
				return false
			}
		}
	}
	var result error
	if end.Code != OK {
		result = &Status{Code: end.Code, Message: end.Message}
	}
	if c.single {
		finishChannel(c.link.conn)
	}
	p.finish(result, false)
	p.retire()
	return true
}

type Stream struct {
	pending *pending
	recvMu  sync.Mutex
}

// Recv drains ordered chunks before a remote terminal error/EOF. Local
// cancellation discards buffered chunks. Concurrent Recv calls are serialized.
func (s *Stream) Recv(response any) error {
	s.recvMu.Lock()
	defer s.recvMu.Unlock()
	body, release, err := s.take()
	if err != nil {
		return err
	}
	defer release()
	if response == nil {
		return nil
	}
	if err := json.Unmarshal(body, response); err != nil {
		_ = s.Close()
		return err
	}
	return nil
}
func (s *Stream) take() (json.RawMessage, func(), error) {
	p := s.pending
	for {
		p.mu.Lock()
		if p.discard {
			err := p.err
			p.mu.Unlock()
			return nil, nil, err
		}
		if len(p.queue) > 0 {
			body := p.queue[0]
			p.queue[0] = receivedChunk{}
			p.queue = p.queue[1:]
			if len(p.queue) == 0 {
				p.client.resources.buffers.remove(p)
			}
			p.mu.Unlock()
			p.creditMu.Lock()
			p.mu.Lock()
			ended, discard, result := p.remoteEnded, p.discard, p.err
			p.mu.Unlock()
			if !ended && !discard {
				c := p.client
				touched, e := c.link.send(p.ctx, wire.Frame{Kind: wire.Credit, ID: p.id, Number: 1})
				if e != nil {
					p.mu.Lock()
					ended, discard, result = p.remoteEnded, p.discard, p.err
					p.mu.Unlock()
					if touched {
						c.fail(e)
					}
					if !ended || discard {
						p.creditMu.Unlock()
						if result != nil {
							body.release()
							return nil, nil, result
						}
						body.release()
						return nil, nil, e
					}
				}
			}
			p.creditMu.Unlock()
			p.mu.Lock()
			discard, result = p.discard, p.err
			p.mu.Unlock()
			if discard {
				body.release()
				return nil, nil, result
			}
			return body.body, body.release, nil
		}
		if p.done {
			err := p.err
			p.mu.Unlock()
			if err == nil {
				err = io.EOF
			}
			return nil, nil, err
		}
		changed := p.changed
		p.mu.Unlock()
		<-changed
	}
}
func (s *Stream) Close() error {
	p := s.pending
	p.mu.Lock()
	done := p.done
	if done && len(p.queue) > 0 {
		p.dropLocked(context.Canceled)
	}
	p.mu.Unlock()
	if done {
		return nil
	}
	p.cancel()
	for {
		p.mu.Lock()
		done = p.done
		changed := p.changed
		if done && len(p.queue) > 0 {
			p.dropLocked(context.Canceled)
		}
		p.mu.Unlock()
		if done {
			return nil
		}
		<-changed
	}
}
