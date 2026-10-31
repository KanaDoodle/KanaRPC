package kanarpc

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

func TestTCPVectoredWriteCancellationReleasesReservation(t *testing.T) {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require(t, err)
	defer listener.Close()
	conn, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	require(t, err)
	defer conn.Close()
	peer, err := listener.AcceptTCP()
	require(t, err)
	defer peer.Close()
	require(t, conn.SetWriteBuffer(1024))
	require(t, peer.SetReadBuffer(1024))
	cfg, err := normalizeIO(IOConfig{MaxFrameBytes: 8 << 20})
	require(t, err)
	memory, err := memoryConfig(MemoryConfig{})
	require(t, err)
	link := newLink(conn, cfg)
	link.resources = newResources(memory)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		touched, err := link.send(ctx, wire.Frame{Kind: wire.Data, ID: 1, Number: 1, Payload: bytes.Repeat([]byte{'x'}, 4<<20)})
		if !touched && err == nil {
			err = errors.New("write did not touch socket")
		}
		done <- err
	}()
	eventually(t, func() bool { return link.resources.send.snapshot().Used == (4<<20)+wire.HeaderSize })
	cancel()
	select {
	case err = <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("writev ignored cancellation")
	}
	if stats := link.resources.snapshot(); stats.Send.Used != 0 || stats.Send.Peak != (4<<20)+wire.HeaderSize {
		t.Fatal(stats)
	}
}
