package kanarpc

import (
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

type UnaryHandler func(context.Context, json.RawMessage) (json.RawMessage, error)
type StreamHandler func(context.Context, json.RawMessage, *Sender) error
type method struct {
	encoding  payloadEncoding
	streaming bool
	run       StreamHandler
}
type serverState uint8

const (
	fresh serverState = iota
	serving
	draining
	closed
)

type ServerStats struct {
	// Canceled is the legacy handler-status count, including deadline statuses.
	// Outcomes records protocol observations separately from application codes.
	Outcomes                                                          ServerOutcomes
	Queued                                                            int
	IncompleteExchanges                                               uint64
	NativeStreams                                                     int
	Connections, InFlight, Executing                                  int
	Admitted, Completed, Rejected, Canceled, Delivered, LostTerminals uint64
}

// Server owns a listener after Serve succeeds, every accepted connection,
// and every admitted handler. Handler registration freezes at Serve.
type Server struct {
	cfg            ServerConfig
	mu             sync.Mutex
	state          serverState
	listener       io.Closer
	accepting      bool
	changed        chan struct{}
	methods        map[string]method
	connections    map[*serverConn]struct{}
	nativeSessions map[*quicServerSession]struct{}
	nativeListener *QUICListener
	stats          ServerStats
	resources      *resources
	governor       *governor
}

func NewServer(cfg ServerConfig) (*Server, error) {
	cfg, err := normalizeServer(cfg)
	if err != nil {
		return nil, err
	}
	g, err := newGovernor(cfg.Governance)
	if err != nil {
		return nil, err
	}
	return &Server{cfg: cfg, changed: make(chan struct{}), methods: make(map[string]method), connections: make(map[*serverConn]struct{}), nativeSessions: make(map[*quicServerSession]struct{}), resources: newResources(cfg.Memory), governor: g}, nil
}
func (s *Server) notifyLocked()      { close(s.changed); s.changed = make(chan struct{}) }
func (s *Server) Stats() ServerStats { s.mu.Lock(); defer s.mu.Unlock(); return s.stats }
func (s *Server) Ready() bool        { s.mu.Lock(); defer s.mu.Unlock(); return s.state == serving }

func (s *Server) HandleUnary(path string, h UnaryHandler) error {
	if h == nil {
		return errors.New("kanarpc: nil handler")
	}
	return s.register(path, method{run: func(ctx context.Context, b json.RawMessage, out *Sender) error {
		response, err := h(ctx, b)
		if err != nil {
			return err
		}
		return out.Send(response)
	}})
}
func (s *Server) HandleStream(path string, h StreamHandler) error {
	if h == nil {
		return errors.New("kanarpc: nil handler")
	}
	return s.register(path, method{streaming: true, run: h})
}
func (s *Server) register(path string, h method) error {
	return s.registerMethods(map[string]method{path: h})
}

func (s *Server) registerMethods(methods map[string]method) error {
	for path := range methods {
		if !validMethod(path) {
			return errors.New("kanarpc: expected /service/method")
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != fresh {
		return errors.New("kanarpc: registration is frozen")
	}
	for path := range methods {
		if _, ok := s.methods[path]; ok {
			return errors.New("kanarpc: duplicate method")
		}
	}
	for path, h := range methods {
		s.methods[path] = h
		if s.governor != nil {
			s.governor.add(path)
		}
	}
	return nil
}

// Serve transfers listener ownership only if the server has not started or
// closed. It returns when acceptance stops; Drain/Wait cover admitted work.
func (s *Server) Serve(listener net.Listener) error {
	return s.serveRecords(listener, nil, 0)
}

func (s *Server) serveRecords(listener net.Listener, security *tls.Config, handshakeTimeout time.Duration) error {
	if listener == nil {
		return errors.New("kanarpc: nil listener")
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
	s.state, s.listener, s.accepting = serving, listener, true
	s.notifyLocked()
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.accepting = false; s.notifyLocked(); s.mu.Unlock() }()
	for {
		conn, err := listener.Accept()
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
		if s.state != serving || len(s.connections) >= s.cfg.MaxConnections {
			s.mu.Unlock()
			closeSocket(conn)
			continue
		}
		ctx, cancel := context.WithCancelCause(context.Background())
		peer := &serverConn{server: s, link: newLink(conn, s.cfg.IO), ctx: ctx, cancel: cancel, requests: make(map[uint64]*execution)}
		peer.traceChannel = s.cfg.Trace.newChannel()
		if security != nil {
			tlsConn := tls.Server(conn, security)
			peer.link = newLink(tlsConn, s.cfg.IO)
			peer.handshake = func() error {
				budget, stop := context.WithTimeout(ctx, handshakeTimeout)
				defer stop()
				if err := tlsConn.HandshakeContext(budget); err != nil {
					return err
				}
				if tlsConn.ConnectionState().NegotiatedProtocol != tlsALPN {
					return ErrProtocol
				}
				return nil
			}
		}
		peer.link.resources = s.resources
		s.connections[peer] = struct{}{}
		s.stats.Connections++
		s.notifyLocked()
		s.mu.Unlock()
		go peer.readLoop()
	}
}

func (s *Server) Close() {
	s.mu.Lock()
	s.state = closed
	l := s.listener
	peers := make([]*serverConn, 0, len(s.connections))
	for p := range s.connections {
		peers = append(peers, p)
	}
	native := make([]*quicServerSession, 0, len(s.nativeSessions))
	for p := range s.nativeSessions {
		native = append(native, p)
	}
	owner := s.nativeListener
	s.notifyLocked()
	s.mu.Unlock()
	if l != nil {
		_ = l.Close()
	}
	for _, p := range peers {
		p.close(ErrClosed)
	}
	for _, p := range native {
		p.close()
	}
	if owner != nil {
		_ = owner.Close()
	}
}

// Wait joins owned connection readers and handlers, including handlers that
// ignore cancellation. It never claims to forcibly terminate Go functions.
func (s *Server) Wait(ctx context.Context) error {
	for {
		s.mu.Lock()
		done := !s.accepting && s.stats.Connections == 0 && s.stats.InFlight == 0 && s.stats.NativeStreams == 0
		changed := s.changed
		s.mu.Unlock()
		if done {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Drain seals admission under the same lock used by admit. Successful normal
// results are kept alive through receipt ACK; the caller deadline bounds drain.
func (s *Server) Drain(ctx context.Context) error {
	_, err := s.DrainWithReport(ctx)
	return err
}

type serverConn struct {
	server       *Server
	link         *link
	ctx          context.Context
	cancel       context.CancelCauseFunc
	once         sync.Once
	mu           sync.Mutex
	requests     map[uint64]*execution
	lastOpen     uint64
	helloSeen    bool
	handshake    func() error
	native       *quicServerSession
	bound        bool
	peerFIN      chan struct{}
	demand       map[string]int
	traceChannel uint64
}

func (p *serverConn) close(err error) {
	p.once.Do(func() { p.cancel(err); closeSocket(p.link.conn) })
}
func (p *serverConn) finish() { p.once.Do(func() { p.cancel(ErrClosed); finishChannel(p.link.conn) }) }
func (p *serverConn) readLoop() {
	defer func() {
		p.close(ErrClosed)
		s := p.server
		s.mu.Lock()
		if p.native != nil {
			s.stats.NativeStreams--
		} else {
			delete(s.connections, p)
			s.stats.Connections--
		}
		s.notifyLocked()
		s.mu.Unlock()
	}()
	if p.handshake != nil {
		if err := p.handshake(); err != nil {
			p.close(err)
			return
		}
	}
	for {
		var f wire.Frame
		var err error
		if p.native != nil && p.lastOpen == 0 {
			f, err = p.link.readWithin(p.native.firstRecordTimeout)
		} else {
			f, err = p.link.read()
		}
		if err != nil {
			if p.native != nil && errors.Is(err, io.EOF) {
				close(p.peerFIN)
				p.trace(p.lastOpen, "", "peer_fin", "native_send_direction_finished", OK, 0)
			}
			p.close(err)
			return
		}
		if !p.handleFrame(f) {
			p.close(ErrProtocol)
			return
		}
	}
}

func (p *serverConn) handleFrame(f wire.Frame) bool {
	retained := false
	defer func() {
		if !retained {
			f.Release()
		}
	}()
	if f.Kind == wire.Hello {
		if err := p.hello(f); err != nil {
			p.close(err)
			return false
		}
		if p.native != nil {
			p.finish()
			return false
		}
		return true
	}
	if f.Kind == wire.SessionBarrier && p.native != nil && p.lastOpen == 0 {
		p.native.barrier(p, f)
		return false
	}
	if f.Kind == wire.Open || f.Kind == wire.OpenEncoded {
		if p.native != nil {
			p.server.mu.Lock()
			p.native.used = true
			if p.native.frameLimit != 0 {
				p.link.cfg.MaxFrameBytes = p.native.frameLimit
			}
			p.server.mu.Unlock()
		}
		if f.ID <= p.lastOpen || p.native != nil && p.lastOpen != 0 {
			return false
		}
		p.lastOpen = f.ID
		var err error
		retained, err = p.open(f)
		if err != nil {
			p.close(err) // Preserve a local rejection-write failure, not ErrProtocol.
			return false
		}
		if p.native != nil && !p.bound {
			p.finish()
			return false
		}
		return true
	}
	if f.ID > p.lastOpen {
		return false
	}
	p.mu.Lock()
	r := p.requests[f.ID]
	p.mu.Unlock()
	switch f.Kind {
	case wire.Cancel:
		if r != nil {
			r.cancelRequest()
		}
	case wire.Credit:
		if r != nil && !r.credit() {
			return false
		}
	case wire.Ack:
		if r != nil && !r.acknowledge(f.Payload) {
			return false
		}
	default:
		return false
	}
	return true
}

func (p *serverConn) reject(id uint64, code Code, reason string, path ...string) error {
	s := p.server
	s.mu.Lock()
	s.stats.Rejected++
	switch reason {
	case "capacity":
		s.stats.Outcomes.CapacityRejected++
	case "policy":
		s.stats.Outcomes.PolicyRejected++
	case "draining":
		s.stats.Outcomes.DrainingRejected++
	case "invalid":
		s.stats.Outcomes.InvalidRejected++
	case "not_found":
		s.stats.Outcomes.NotFoundRejected++
	}
	s.mu.Unlock()
	p.trace(id, "", "rejected", reason, code, 0)
	var pressure *Pressure
	if len(path) > 0 {
		pressure = s.pressure(p, path[0])
	}
	b := marshalEnd(endRecord{Status: Status{Code: code}, Pressure: pressure}, p.link.cfg.MaxFrameBytes)
	ctx, cancel := context.WithTimeout(p.ctx, s.cfg.IO.WriteTimeout)
	defer cancel()
	_, err := p.link.send(ctx, wire.Frame{Kind: wire.End, ID: id, Payload: b})
	return err
}
func (p *serverConn) open(f wire.Frame) (bool, error) {
	request, encoding, decodeErr := decodeOpen(f)
	if p.server.cfg.Trace != nil {
		traceMethod := request.Method
		if !validMethod(traceMethod) {
			traceMethod = ""
		}
		p.trace(f.ID, traceMethod, "open", "record_parsed", OK, 0)
	}
	if decodeErr != nil || uint64(len(f.Payload)) > uint64(p.link.cfg.MaxFrameBytes) || !validMethod(request.Method) || request.BudgetNanos <= 0 || request.Window < 1 || request.Window > 32 {
		return false, p.reject(f.ID, InvalidArgument, "invalid")
	}
	if !request.Stream && request.Window != 1 {
		return false, p.reject(f.ID, InvalidArgument, "invalid")
	}
	s := p.server
	s.mu.Lock()
	if s.state != serving || p.native != nil && p.native.stopping {
		s.mu.Unlock()
		return false, p.reject(f.ID, Unavailable, "draining", request.Method)
	}
	h, ok := s.methods[request.Method]
	if !ok {
		s.mu.Unlock()
		return false, p.reject(f.ID, NotFound, "not_found")
	}
	if h.streaming != request.Stream || h.encoding != encoding {
		s.mu.Unlock()
		return false, p.reject(f.ID, InvalidArgument, "invalid")
	}
	p.mu.Lock()
	count := len(p.requests)
	if p.native != nil {
		count = p.native.admitted
	}
	if s.stats.InFlight >= s.cfg.MaxInFlight || count >= s.cfg.MaxPerConnection {
		p.mu.Unlock()
		s.mu.Unlock()
		return false, p.reject(f.ID, ResourceExhausted, "capacity", request.Method)
	}
	if s.governor != nil && !s.canAdmitLocked(request.Method) {
		p.mu.Unlock()
		s.mu.Unlock()
		return false, p.reject(f.ID, ResourceExhausted, "policy", request.Method)
	}
	duration := time.Duration(request.BudgetNanos)
	if duration > s.cfg.MaxRequestDuration {
		duration = s.cfg.MaxRequestDuration
	}
	budget, stop := context.WithTimeout(p.ctx, duration)
	ctx, cancel := context.WithCancelCause(budget)
	if s.cfg.Trace != nil {
		ctx = context.WithValue(ctx, requestTraceKey{}, TraceRef{p.traceChannel, f.ID, request.Method})
	}
	r := &execution{encoding: encoding, peer: p, id: f.ID, ctx: ctx, cancel: cancel, stop: stop, window: request.Window, credits: request.Window, changed: make(chan struct{}), ack: make(chan struct{}), input: f, path: request.Method}
	p.requests[f.ID] = r
	p.bound = true
	if p.native != nil {
		p.native.admitted++
	}
	s.stats.InFlight++
	s.stats.Admitted++
	r.trace("admitted", "request_capacity_reserved", OK, 0)
	s.demandLocked(p, r.path, 1)
	s.notifyLocked()
	p.mu.Unlock()
	if s.governor == nil {
		s.stats.Executing++
		r.started = time.Now()
		go r.run(h, request.Request)
	} else {
		s.enqueueLocked(r, h, request.Request)
	}
	s.mu.Unlock()
	return true, nil
}

type execution struct {
	peer                                *serverConn
	id                                  uint64
	ctx                                 context.Context
	cancel                              context.CancelCauseFunc
	stop                                context.CancelFunc
	sendMu                              sync.Mutex
	mu                                  sync.Mutex
	window, credits, sequence, returned uint32
	changed                             chan struct{}
	terminal                            bool
	encoding                            payloadEncoding
	receipt                             [16]byte
	ack                                 chan struct{}
	ackOnce                             sync.Once
	input                               wire.Frame
	path                                string
	handler                             method
	body                                []byte
	jobState                            uint8
	queuedAt, queueDeadline, started    time.Time
	queueWait                           time.Duration
	queueStop                           func() bool
	queueCancel                         context.CancelFunc
	queueError                          error
	controlGeneration                   uint64
	cancelObserved                      bool // server.mu
	receiptResolved                     bool // mu
}

// Sender is valid only during its handler invocation. Send is serialized and
// waits for receiver credits without blocking the connection's control reader.
// Body is preencoded using the registered method's codec. Protobuf handlers
// normally use the typed send callback instead of calling Sender directly.
type Sender struct{ request *execution }

func (out *Sender) Send(body json.RawMessage) error {
	r := out.request
	if r.encoding == encodingJSON && !json.Valid(body) || uint64(len(body)) > uint64(r.peer.link.cfg.MaxFrameBytes) {
		return &Status{Code: InvalidArgument, Message: "invalid or oversized response"}
	}
	release, err := r.peer.server.resources.send.take(int64(len(body)))
	if err != nil {
		if r.peer.server.cfg.Trace != nil {
			r.trace("budget_rejected", "local_send_reservation", statusOf(err).Code, 0)
		}
		return err
	}
	defer release()
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	for {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		r.mu.Lock()
		if r.terminal {
			r.mu.Unlock()
			return ErrClosed
		}
		if r.credits > 0 {
			if r.sequence == math.MaxUint32 {
				r.mu.Unlock()
				return &Status{Code: ResourceExhausted, Message: "stream sequence exhausted"}
			}
			r.credits--
			r.sequence++
			number := r.sequence
			r.mu.Unlock()
			kind := wire.Data
			if r.encoding == encodingProtobuf {
				kind = wire.DataEncoded
			}
			touched, err := r.peer.link.send(r.ctx, wire.Frame{Kind: kind, ID: r.id, Number: number, Payload: body})
			if err == nil {
				r.trace("data_sent", "credit_consumed", OK, int64(number))
			}
			if err != nil && touched {
				r.peer.close(err)
			}
			return err
		}
		changed := r.changed
		r.mu.Unlock()
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		case <-changed:
		}
	}
}
func (r *execution) credit() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.terminal {
		return true
	}
	if r.returned >= r.sequence || r.credits >= r.window {
		return false
	}
	r.returned++
	r.credits++
	close(r.changed)
	r.changed = make(chan struct{})
	return true
}
func (r *execution) acknowledge(receipt []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.terminal || string(r.receipt[:]) != string(receipt) {
		return false
	}
	if r.receiptResolved {
		r.trace("late_ack", "settlement_already_frozen", OK, 0)
		return true // A receipt after settlement cannot rewrite historical loss.
	}
	r.ackOnce.Do(func() { close(r.ack); r.trace("ack_received", "protocol_receipt_observed", OK, 0) })
	return true
}

func invoke(h method, ctx context.Context, b json.RawMessage, out *Sender) (err error) {
	defer func() {
		if recover() != nil {
			err = &Status{Code: Internal, Message: "handler panicked"}
		}
	}()
	return h.run(ctx, b, out)
}
func (r *execution) run(h method, body json.RawMessage) {
	s := r.peer.server
	defer func() {
		r.recordStop()
		r.cancel(ErrClosed)
		r.stop()
		// Admission takes server then peer; cleanup follows the same lock order.
		s.mu.Lock()
		r.peer.mu.Lock()
		delete(r.peer.requests, r.id)
		if r.peer.native != nil {
			r.peer.native.admitted--
		}
		r.peer.mu.Unlock()
		s.stats.InFlight--
		s.stats.Completed++
		r.trace("retired", "server_request_released", OK, 0)
		s.notifyLocked()
		s.mu.Unlock()
		if r.peer.native != nil {
			r.peer.finish()
		}
	}()
	err := r.queueError
	var executionDuration time.Duration
	invokedHandler := false
	if !r.started.IsZero() {
		err = deadlineError(r.ctx)
		if err == nil {
			r.trace("handler_started", "execution_slot_acquired", OK, 0)
			invoked := time.Now()
			invokedHandler = true
			err = invoke(h, r.ctx, body, &Sender{request: r})
			executionDuration = time.Since(invoked)
		}
	}
	body = nil
	r.body = nil
	r.input.Release()
	if r.ctx.Err() != nil {
		err = r.ctx.Err()
	}
	classified := statusOf(err)
	if invokedHandler {
		r.trace("handler_done", "handler_returned", classified.Code, 0)
	} else {
		r.trace("execution_skipped", "dispatch_context_finished", classified.Code, 0)
	}
	s.mu.Lock()
	if !r.started.IsZero() {
		s.handlerDoneLocked(r, classified, executionDuration)
	}
	if classified.Code == Canceled || classified.Code == DeadlineExceeded {
		s.stats.Canceled++
	}
	s.notifyLocked()
	s.mu.Unlock()
	if r.ctx.Err() == context.Canceled || r.peer.ctx.Err() != nil {
		return
	}
	r.sendMu.Lock()
	defer r.sendMu.Unlock()
	r.mu.Lock()
	r.terminal = true
	status := endRecord{Status: classified}
	wantACK := r.ctx.Err() == nil
	if wantACK {
		if _, e := rand.Read(r.receipt[:]); e != nil {
			r.mu.Unlock()
			r.peer.close(e)
			return
		}
		status.Receipt = hex.EncodeToString(r.receipt[:])
	}
	r.mu.Unlock()
	r.trace("terminal_ready", "handler_result_classified", classified.Code, 0)
	status.Pressure = s.pressure(r.peer, r.path)
	b := marshalEnd(status, r.peer.link.cfg.MaxFrameBytes)
	ctx, cancel := context.WithTimeout(r.peer.ctx, s.cfg.AckTimeout)
	defer cancel()
	touched, writeErr := r.peer.link.send(ctx, wire.Frame{Kind: wire.End, ID: r.id, Payload: b})
	if writeErr != nil {
		if touched {
			r.peer.close(writeErr)
		}
		if wantACK {
			r.recordDelivery()
		}
		return
	}
	if !wantACK {
		r.trace("terminal_sent", "request_deadline_no_receipt", classified.Code, 0)
		return
	}
	r.trace("terminal_sent", "receipt_expected", classified.Code, 0)
	select {
	case <-r.ack:
	case <-r.ctx.Done():
	case <-ctx.Done():
	}
	acked := r.recordDelivery()
	if r.peer.native != nil && acked {
		channel := r.peer.link.conn.(*nativeChannel)
		_ = channel.Stream.Close()
		select {
		case <-r.peer.peerFIN:
		case <-ctx.Done():
		case <-r.ctx.Done():
		}
		select {
		case <-r.peer.peerFIN:
		default:
			s.mu.Lock()
			if !(r.cancelObserved && context.Cause(r.ctx) == context.Canceled) && r.ctx.Err() != context.DeadlineExceeded {
				s.stats.IncompleteExchanges++
				r.trace("exchange_incomplete", "client_fin_not_observed", Unavailable, 0)
			}
			s.mu.Unlock()
		}
	}
}

func (r *execution) recordDelivery() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.receiptResolved {
		return false
	}
	r.receiptResolved = true
	s := r.peer.server
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.Outcomes.ReceiptsResolved++
	// ACK is an irreversible protocol observation. A later socket close or a
	// racing deadline must not overturn it, even if both select cases are ready.
	select {
	case <-r.ack:
		s.stats.Delivered++
		r.trace("receipt_acknowledged", "delivery_receipt_confirmed", OK, 0)
		return true
	default:
		cause := context.Cause(r.ctx)
		switch {
		case cause == context.Canceled && r.cancelObserved:
			s.stats.Outcomes.ReceiptsCanceled++
			r.trace("receipt_canceled", "explicit_cancel_won", Canceled, 0)
		case r.ctx.Err() == context.DeadlineExceeded:
			s.stats.Outcomes.ReceiptsDeadlineExceeded++
			r.trace("receipt_deadline", "request_deadline", DeadlineExceeded, 0)
		default:
			s.stats.LostTerminals++
			r.trace("receipt_lost", "terminal_not_acknowledged", Unavailable, 0)
		}
	}
	return false
}

// RegisterUnary provides compile-time request/response types without reflection.
func RegisterUnary[Request, Response any](s *Server, path string, h func(context.Context, Request) (Response, error)) error {
	if h == nil {
		return errors.New("kanarpc: nil handler")
	}
	return s.HandleUnary(path, func(ctx context.Context, body json.RawMessage) (json.RawMessage, error) {
		var request Request
		if json.Unmarshal(body, &request) != nil {
			return nil, &Status{Code: InvalidArgument, Message: "request decode failed"}
		}
		response, err := h(ctx, request)
		if err != nil {
			return nil, err
		}
		b, err := json.Marshal(response)
		return b, err
	})
}
func RegisterStream[Request, Response any](s *Server, path string, h func(context.Context, Request, func(Response) error) error) error {
	if h == nil {
		return errors.New("kanarpc: nil handler")
	}
	return s.HandleStream(path, func(ctx context.Context, body json.RawMessage, out *Sender) error {
		var request Request
		if json.Unmarshal(body, &request) != nil {
			return &Status{Code: InvalidArgument, Message: "request decode failed"}
		}
		return h(ctx, request, func(response Response) error {
			b, err := json.Marshal(response)
			if err != nil {
				return fmt.Errorf("response encode: %w", err)
			}
			return out.Send(b)
		})
	})
}
