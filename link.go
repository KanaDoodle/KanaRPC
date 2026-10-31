package kanarpc

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

// A link owns one ordered record writer. No business goroutine holds this
// permit while waiting for credits or for a user handler to return.
type link struct {
	conn      recordChannel
	cfg       IOConfig
	permit    chan struct{}
	resources *resources
}

// recordChannel is one ordered record channel, not necessarily a physical
// connection. QUIC supplies one native bidirectional stream per exchange.
type recordChannel interface {
	io.Reader
	io.Writer
	Close() error
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
}

func newLink(conn recordChannel, cfg IOConfig) *link {
	l := &link{conn: conn, cfg: cfg, permit: make(chan struct{}, 1)}
	l.permit <- struct{}{}
	return l
}
func (l *link) acquire(ctx context.Context) error {
	if err := deadlineError(ctx); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-l.permit:
	}
	if err := deadlineError(ctx); err != nil {
		l.release()
		return err
	}
	return nil
}
func (l *link) release() { l.permit <- struct{}{} }

// writeLocked returns touched=true once a write deadline or record write is
// attempted. Any subsequent failure makes an ordered byte stream unusable.
func deadlineError(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}

func (l *link) writeLocked(ctx context.Context, f wire.Frame) (touched bool, err error) {
	if err = deadlineError(ctx); err != nil {
		return false, err
	}
	if l.resources != nil {
		release, e := l.resources.reserveWrite(f.Kind, len(f.Payload))
		if e != nil {
			return false, e
		}
		defer release()
	}
	deadline := time.Now().Add(l.cfg.WriteTimeout)
	contextBound := false
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
		contextBound = true
	}
	if err = l.conn.SetWriteDeadline(deadline); err != nil {
		return true, err
	}
	callbackDone := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = l.conn.SetWriteDeadline(time.Now()); close(callbackDone) })
	err = wire.Write(l.conn, f, l.cfg.MaxFrameBytes)
	if !stop() {
		<-callbackDone
	}
	// Join the deadline callback before reusing this connection's writer.
	resetErr := l.conn.SetWriteDeadline(time.Time{})
	if err == nil {
		err = resetErr
	}
	if err != nil && ctx.Err() != nil {
		err = ctx.Err()
	} else if err != nil && contextBound && !time.Now().Before(deadline) {
		// The socket timer can fire before the context timer is scheduled.
		// Preserve deadline identity when the caller supplied the IO bound.
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			err = context.DeadlineExceeded
		}
	}
	return true, err
}
func (l *link) send(ctx context.Context, f wire.Frame) (bool, error) {
	if err := l.acquire(ctx); err != nil {
		return false, err
	}
	defer l.release()
	return l.writeLocked(ctx, f)
}
func (l *link) read() (wire.Frame, error) {
	return l.readWithin(l.cfg.ReadTimeout)
}
func (l *link) readWithin(timeout time.Duration) (wire.Frame, error) {
	if timeout > l.cfg.ReadTimeout {
		timeout = l.cfg.ReadTimeout
	}
	if err := l.conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return wire.Frame{}, err
	}
	if l.resources != nil {
		return wire.ReadReserved(l.conn, l.cfg.MaxFrameBytes, l.resources.reserveRead)
	}
	return wire.Read(l.conn, l.cfg.MaxFrameBytes)
}

func closeSocket(conn recordChannel) {
	// TLS's graceful close_notify can wait for a peer that no longer reads.
	// Closing the underlying socket is the bounded ownership operation here.
	for {
		inner, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			break
		}
		next := inner.NetConn()
		if next == conn {
			break
		}
		conn = next
	}
	_ = conn.Close()
}

func finishChannel(conn recordChannel) {
	if channel, ok := conn.(interface{ Finish() }); ok {
		channel.Finish()
		return
	}
	closeSocket(conn)
}
