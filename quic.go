package kanarpc

import (
	"context"
	"crypto/rand"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
	quic "github.com/quic-go/quic-go"
)

const (
	nativeCancel   quic.StreamErrorCode      = 0x721
	nativeProtocol quic.StreamErrorCode      = 0x722
	nativeShutdown quic.ApplicationErrorCode = 0x723
	nativeCapacity quic.ApplicationErrorCode = 0x724
)

type QUICConfig struct {
	MaxStreams         int
	IdleTimeout        time.Duration
	FirstRecordTimeout time.Duration
}

func quicConfig(c QUICConfig, handshake time.Duration) (QUICConfig, *quic.Config, error) {
	if c.MaxStreams == 0 {
		c.MaxStreams = 64
	}
	if c.IdleTimeout == 0 {
		c.IdleTimeout = 30 * time.Second
	}
	if c.FirstRecordTimeout == 0 {
		c.FirstRecordTimeout = 3 * time.Second
	}
	if c.MaxStreams < 1 || c.MaxStreams > 4096 || c.IdleTimeout <= 0 || c.FirstRecordTimeout <= 0 {
		return c, nil, errors.New("kanarpc: invalid QUIC bounds")
	}
	return c, &quic.Config{HandshakeIdleTimeout: handshake, MaxIdleTimeout: c.IdleTimeout, MaxIncomingStreams: int64(c.MaxStreams), MaxIncomingUniStreams: -1, InitialStreamReceiveWindow: 64 << 10, MaxStreamReceiveWindow: 1 << 20, InitialConnectionReceiveWindow: 512 << 10, MaxConnectionReceiveWindow: 8 << 20}, nil
}

// QUICListener explicitly owns a packet socket and transport. Stopping new
// handshakes is separate from closing accepted sessions and the UDP socket.
type QUICListener struct {
	listener  *quic.Listener
	transport *quic.Transport
	packet    net.PacketConn
	cfg       QUICConfig
	once      sync.Once
}

func ListenQUIC(address string, security TLSOptions, options QUICConfig) (*QUICListener, error) {
	tlsConfig, timeout, err := prepareTLS(security, true, quicALPN)
	if err != nil {
		return nil, err
	}
	options, config, err := quicConfig(options, timeout)
	if err != nil {
		return nil, err
	}
	packet, err := net.ListenPacket("udp", address)
	if err != nil {
		return nil, err
	}
	transport := &quic.Transport{Conn: packet}
	listener, err := transport.Listen(tlsConfig, config)
	if err != nil {
		_ = transport.Close()
		_ = packet.Close()
		return nil, err
	}
	return &QUICListener{listener: listener, transport: transport, packet: packet, cfg: options}, nil
}
func (l *QUICListener) Addr() net.Addr { return l.packet.LocalAddr() }
func (l *QUICListener) Close() error {
	l.once.Do(func() { _ = l.listener.Close(); _ = l.transport.Close(); _ = l.packet.Close() })
	return nil
}

type admissionCloser struct{ close func() error }

func (c admissionCloser) Close() error { return c.close() }

func (s *Server) ServeQUIC(listener *QUICListener) error {
	if listener == nil {
		return errors.New("kanarpc: nil QUIC listener")
	}
	s.mu.Lock()
	if s.state != fresh {
		s.mu.Unlock()
		return ErrClosed
	}
	if err := s.validatePoliciesLocked(); err != nil {
		s.mu.Unlock()
		return err
	}
	s.state, s.accepting, s.nativeListener = serving, true, listener
	s.listener = admissionCloser{listener.listener.Close}
	s.notifyLocked()
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.accepting = false; s.notifyLocked(); s.mu.Unlock() }()
	for {
		conn, err := listener.listener.Accept(context.Background())
		if err != nil {
			s.mu.Lock()
			stopped := s.state != serving
			s.mu.Unlock()
			if stopped {
				return nil
			}
			s.Close()
			return err
		}
		s.mu.Lock()
		if s.state != serving || s.stats.Connections >= s.cfg.MaxConnections {
			s.mu.Unlock()
			_ = conn.CloseWithError(nativeCapacity, "session capacity")
			continue
		}
		ctx, cancel := context.WithCancelCause(conn.Context())
		parent := &quicServerSession{server: s, conn: conn, ctx: ctx, cancel: cancel, firstRecordTimeout: listener.cfg.FirstRecordTimeout}
		s.nativeSessions[parent] = struct{}{}
		s.stats.Connections++
		s.notifyLocked()
		s.mu.Unlock()
		go parent.run()
	}
}

type quicServerSession struct {
	server             *Server
	conn               *quic.Conn
	ctx                context.Context
	cancel             context.CancelCauseFunc
	once               sync.Once
	admitted           int  // guarded by server.mu; physical-session execution quota
	stopping           bool // guarded by server.mu
	demand             map[string]int
	firstRecordTimeout time.Duration
}

func (p *quicServerSession) barrier(peer *serverConn, f wire.Frame) {
	s := p.server
	ctx, cancel := context.WithTimeout(p.ctx, s.cfg.AckTimeout)
	defer cancel()
	for {
		s.mu.Lock()
		p.stopping = true
		quiet := p.admitted == 0
		changed := s.changed
		s.mu.Unlock()
		if quiet {
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-changed:
		}
	}
	if _, err := peer.link.send(ctx, f); err == nil {
		peer.finish()
	}
}

func (p *quicServerSession) close() {
	p.once.Do(func() { p.cancel(ErrClosed); _ = p.conn.CloseWithError(nativeShutdown, "server closed") })
}
func (p *quicServerSession) run() {
	s := p.server
	defer func() {
		p.close()
		s.mu.Lock()
		delete(s.nativeSessions, p)
		s.stats.Connections--
		s.notifyLocked()
		s.mu.Unlock()
	}()
	for {
		stream, err := p.conn.AcceptStream(p.ctx)
		if err != nil {
			return
		}
		ctx, cancel := context.WithCancelCause(p.ctx)
		channel := &nativeChannel{Stream: stream}
		peer := &serverConn{server: s, native: p, link: newLink(channel, s.cfg.IO), ctx: ctx, cancel: cancel, requests: make(map[uint64]*execution), peerFIN: make(chan struct{})}
		peer.link.resources = s.resources
		// A stream that never supplies Open must not retain its reader forever.
		s.mu.Lock()
		s.stats.NativeStreams++
		s.notifyLocked()
		s.mu.Unlock()
		go peer.readLoop()
	}
}

// nativeChannel is exactly one QUIC stream. It never represents a session as
// a net.Conn, and a parser/IO error only resets this request's two directions.
type nativeChannel struct {
	*quic.Stream
	once sync.Once
}

func (c *nativeChannel) Read(b []byte) (int, error) {
	n, err := c.Stream.Read(b)
	return n, nativeError(err)
}
func (c *nativeChannel) Write(b []byte) (int, error) {
	n, err := c.Stream.Write(b)
	return n, nativeError(err)
}

func (c *nativeChannel) Close() error {
	c.once.Do(func() { c.CancelRead(nativeProtocol); c.CancelWrite(nativeProtocol) })
	return nil
}
func (c *nativeChannel) Finish() {
	c.once.Do(func() { _ = c.Stream.Close(); c.CancelRead(nativeCancel) })
}
func (c *nativeChannel) AwaitEnd(ctx context.Context) error {
	deadline, _ := ctx.Deadline()
	if err := c.SetReadDeadline(deadline); err != nil {
		return err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = c.SetReadDeadline(time.Now()); close(done) })
	var one [1]byte
	n, err := c.Read(one[:])
	if !stop() {
		<-done
	}
	if n == 0 && err == io.EOF {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if err == nil {
		return ErrProtocol
	}
	return nativeError(err)
}
func nativeError(err error) error {
	var application *quic.ApplicationError
	if errors.As(err, &application) {
		switch application.ErrorCode {
		case nativeCapacity:
			return &Status{Code: ResourceExhausted, Message: "QUIC session capacity"}
		case nativeShutdown:
			return &Status{Code: Unavailable, Message: "QUIC session closed"}
		}
	}
	var stream *quic.StreamError
	if errors.As(err, &stream) && stream.ErrorCode == nativeProtocol {
		return ErrProtocol
	}
	return err
}

type QUICStats struct {
	CloseConfirmed                        bool
	Active                                int
	Opened, Retired                       uint64
	RTT                                   time.Duration
	BytesSent, BytesReceived, PacketsLost uint64
}

// QUICClient owns one authenticated physical session. Each Call/Stream opens
// an independent native exchange, with no retries or hidden TCP downgrade.
type QUICClient struct {
	resources *resources
	conn      *quic.Conn
	cfg       ClientConfig
	mu        sync.Mutex
	closed    bool
	children  map[*Client]struct{}
	stats     QUICStats
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
}

func DialQUIC(ctx context.Context, address string, security TLSOptions, options QUICConfig, cfg ClientConfig) (*QUICClient, error) {
	var err error
	if cfg, err = normalizeClient(cfg); err != nil {
		return nil, err
	}
	tlsConfig, timeout, err := prepareTLS(security, false, quicALPN)
	if err != nil {
		return nil, err
	}
	_, config, err := quicConfig(options, timeout)
	if err != nil {
		return nil, err
	}
	config.MaxIncomingStreams = -1 // application is strictly client-initiated
	if cfg.Timeout < timeout {
		timeout = cfg.Timeout
	}
	budget, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := quic.DialAddr(budget, address, tlsConfig, config)
	if err != nil {
		return nil, nativeError(err)
	}
	owner, stop := context.WithCancel(context.Background())
	res := newResources(cfg.Memory)
	res.feedback = newFeedback(cfg.Feedback, cfg.MaxInFlight)
	return &QUICClient{conn: conn, cfg: cfg, children: make(map[*Client]struct{}), ctx: owner, cancel: stop, resources: res}, nil
}
func (c *QUICClient) Stats() QUICStats {
	c.mu.Lock()
	stats := c.stats
	c.mu.Unlock()
	wire := c.conn.ConnectionStats()
	stats.RTT = wire.SmoothedRTT
	stats.BytesSent = wire.BytesSent
	stats.BytesReceived = wire.BytesReceived
	stats.PacketsLost = wire.PacketsLost
	return stats
}
func (c *QUICClient) exchange(ctx context.Context) (*Client, context.Context, error) {
	budget, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	stopClosing := context.AfterFunc(c.ctx, cancel)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		cancel()
		stopClosing()
		return nil, nil, ErrClosed
	}
	if c.stats.Active >= c.cfg.MaxInFlight {
		c.mu.Unlock()
		cancel()
		stopClosing()
		return nil, nil, &Status{Code: ResourceExhausted, Message: "QUIC in-flight limit"}
	}
	c.stats.Active++
	c.wg.Add(1)
	c.mu.Unlock()
	stream, err := c.conn.OpenStreamSync(budget)
	if err != nil {
		c.mu.Lock()
		c.stats.Active--
		c.mu.Unlock()
		c.wg.Done()
		cancel()
		stopClosing()
		return nil, nil, nativeError(err)
	}
	childConfig := c.cfg
	childConfig.MaxInFlight = 1
	child, err := newRecordClient(&nativeChannel{Stream: stream}, childConfig, true, c.resources)
	if err != nil {
		stream.CancelRead(nativeCancel)
		stream.CancelWrite(nativeCancel)
		c.mu.Lock()
		c.stats.Active--
		c.mu.Unlock()
		c.wg.Done()
		cancel()
		stopClosing()
		return nil, nil, err
	}
	c.mu.Lock()
	c.children[child] = struct{}{}
	c.stats.Opened++
	c.mu.Unlock()
	go func() {
		<-child.ctx.Done()
		child.stopIO()
		cancel()
		stopClosing()
		c.mu.Lock()
		delete(c.children, child)
		c.stats.Active--
		c.stats.Retired++
		c.mu.Unlock()
		c.wg.Done()
	}()
	return child, budget, nil
}
func (c *QUICClient) Call(ctx context.Context, method string, request, response any) error {
	child, budget, err := c.exchange(ctx)
	if err != nil {
		return err
	}
	err = child.Call(budget, method, request, response)
	if err != nil {
		child.fail(err)
	}
	return nativeError(err)
}
func (c *QUICClient) Stream(ctx context.Context, method string, request any) (*Stream, error) {
	child, budget, err := c.exchange(ctx)
	if err != nil {
		return nil, err
	}
	stream, err := child.Stream(budget, method, request)
	if err != nil {
		child.fail(err)
	}
	return stream, nativeError(err)
}
func (c *QUICClient) Close() {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		children := make([]*Client, 0, len(c.children))
		for child := range c.children {
			children = append(children, child)
		}
		c.mu.Unlock()
		c.cancel()
		for _, child := range children {
			child.fail(ErrClosed)
		}
		c.wg.Wait()
		c.resources.buffers.clear()
		confirmed := c.closeBarrier()
		c.mu.Lock()
		c.stats.CloseConfirmed = confirmed
		c.mu.Unlock()
		_ = c.conn.CloseWithError(nativeShutdown, "client closed")
	})
}

func (c *QUICClient) closeBarrier() bool {
	ctx, cancel := context.WithTimeout(context.Background(), c.cfg.ControlTimeout)
	defer cancel()
	stream, err := c.conn.OpenStreamSync(ctx)
	if err != nil {
		return false
	}
	defer func() { stream.CancelRead(nativeCancel); stream.CancelWrite(nativeCancel) }()
	deadline, _ := ctx.Deadline()
	_ = stream.SetDeadline(deadline)
	var nonce [16]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		return false
	}
	f := wire.Frame{Kind: wire.SessionBarrier, ID: 1, Payload: nonce[:]}
	release, err := c.resources.reserveWrite(f.Kind, len(f.Payload))
	if err != nil {
		return false
	}
	err = wire.Write(stream, f, c.cfg.IO.MaxFrameBytes)
	release()
	if err != nil {
		return false
	}
	reply, err := wire.ReadReserved(stream, c.cfg.IO.MaxFrameBytes, c.resources.reserveRead)
	defer reply.Release()
	return err == nil && reply.Kind == wire.SessionBarrier && reply.ID == f.ID && string(reply.Payload) == string(nonce[:])
}
