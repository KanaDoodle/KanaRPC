package kanarpc

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

type Feature uint32

const (
	FeatureProtobuf Feature = 1 << iota
	FeatureServerStreaming
	FeatureCallOptions
	supportedFeatures = FeatureProtobuf | FeatureServerStreaming | FeatureCallOptions
)

var ErrCapabilities = errors.New("kanarpc: required protocol capabilities unavailable")

// CapabilityError precedes business dispatch. It never authorizes a retry of
// another call. Cause preserves cancellation, deadlines and handshake failures.
type CapabilityError struct {
	Required, Supported Feature
	Cause               error
}

func (e *CapabilityError) Error() string {
	return fmt.Sprintf("%v: required=%#x supported=%#x (%v)", ErrCapabilities, e.Required, e.Supported, e.Cause)
}
func (e *CapabilityError) Unwrap() error { return errors.Join(ErrCapabilities, e.Cause) }

type Capabilities struct {
	Negotiated    bool
	Features      Feature
	MaxFrameBytes uint32
}

func (c *Client) Capabilities() Capabilities     { return c.capabilities }
func (c *QUICClient) Capabilities() Capabilities { return c.capabilities }

// Negotiation is one bounded exchange before the connection is published.
// Payload: nonce[16], feature bits[4], frame limit[4], version=1[4], reserved[4].
func negotiate(ctx context.Context, l *link, cfg ClientConfig) (caps Capabilities, result error) {
	budget, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()
	var payload [32]byte
	if _, err := rand.Read(payload[:16]); err != nil {
		return caps, err
	}
	binary.BigEndian.PutUint32(payload[16:20], uint32(cfg.RequiredFeatures))
	binary.BigEndian.PutUint32(payload[20:24], cfg.IO.MaxFrameBytes)
	binary.BigEndian.PutUint32(payload[24:28], 1)
	defer func() {
		if result != nil {
			result = &CapabilityError{Required: cfg.RequiredFeatures, Supported: caps.Features, Cause: result}
		}
	}()
	if _, err := l.send(budget, wire.Frame{Kind: wire.Hello, ID: 1, Payload: payload[:]}); err != nil {
		return caps, err
	}
	deadline, _ := budget.Deadline()
	if err := l.conn.SetReadDeadline(deadline); err != nil {
		return caps, err
	}
	done := make(chan struct{})
	stop := context.AfterFunc(budget, func() { _ = l.conn.SetReadDeadline(time.Now()); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		if err := l.conn.SetReadDeadline(time.Time{}); result == nil {
			result = err
		}
	}()
	f, err := wire.ReadReserved(l.conn, cfg.IO.MaxFrameBytes, func(kind wire.Kind, n uint32) (func(), error) {
		if kind != wire.Welcome || n != 32 {
			return nil, ErrProtocol
		}
		return l.resources.reserveRead(kind, n)
	})
	if err != nil {
		if budget.Err() != nil {
			err = budget.Err()
		} else if !time.Now().Before(deadline) {
			err = context.DeadlineExceeded
		}
		return caps, err
	}
	defer f.Release()
	if f.ID != 1 || string(f.Payload[:16]) != string(payload[:16]) || binary.BigEndian.Uint32(f.Payload[24:28]) != 1 || binary.BigEndian.Uint32(f.Payload[28:32]) != 0 {
		return caps, ErrProtocol
	}
	caps.Features = Feature(binary.BigEndian.Uint32(f.Payload[16:20]))
	caps.MaxFrameBytes = binary.BigEndian.Uint32(f.Payload[20:24])
	if caps.MaxFrameBytes < 1024 || caps.MaxFrameBytes > cfg.IO.MaxFrameBytes {
		return caps, ErrProtocol
	}
	if caps.Features&cfg.RequiredFeatures != cfg.RequiredFeatures {
		return caps, errors.New("peer does not advertise all requested features")
	}
	caps.Negotiated = true
	return caps, nil
}

func (p *serverConn) hello(f wire.Frame) error {
	if len(f.Payload) != 32 || f.ID != 1 || p.lastOpen != 0 || p.helloSeen || binary.BigEndian.Uint32(f.Payload[24:28]) != 1 || binary.BigEndian.Uint32(f.Payload[28:32]) != 0 {
		return ErrProtocol
	}
	limit := binary.BigEndian.Uint32(f.Payload[20:24])
	if limit < 1024 || limit > wire.HardLimit {
		return ErrProtocol
	}
	if limit > p.server.cfg.IO.MaxFrameBytes {
		limit = p.server.cfg.IO.MaxFrameBytes
	}
	s := p.server
	s.mu.Lock()
	if s.state != serving || p.native != nil && (p.native.helloSeen || p.native.used || p.native.stopping) {
		s.mu.Unlock()
		return ErrDraining
	}
	if p.native != nil {
		p.native.helloSeen, p.native.frameLimit = true, limit
	}
	s.mu.Unlock()
	p.helloSeen, p.link.cfg.MaxFrameBytes = true, limit
	var reply [32]byte
	copy(reply[:16], f.Payload[:16])
	binary.BigEndian.PutUint32(reply[16:20], uint32(supportedFeatures))
	binary.BigEndian.PutUint32(reply[20:24], limit)
	binary.BigEndian.PutUint32(reply[24:28], 1)
	ctx, cancel := context.WithTimeout(p.ctx, s.cfg.IO.WriteTimeout)
	defer cancel()
	_, err := p.link.send(ctx, wire.Frame{Kind: wire.Welcome, ID: 1, Payload: reply[:]})
	return err
}
