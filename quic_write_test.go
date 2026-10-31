package kanarpc

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/testcert"
	"github.com/KanaDoodle/KanaRPC/internal/wire"
	quic "github.com/quic-go/quic-go"
)

// Raw peer gives precise control over stream credit without changing the
// production QUIC configuration or injecting a fake writer into nativeChannel.
func nativeWritePair(t testing.TB) (*nativeChannel, *quic.Stream, *QUICClient) {
	t.Helper()
	credentials, e := testcert.New()
	require(t, e)
	listener, e := ListenQUIC("127.0.0.1:0", TLSOptions{Config: credentials.Server}, QUICConfig{MaxStreams: 4})
	require(t, e)
	t.Cleanup(func() { listener.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accepted := make(chan *quic.Conn, 1)
	failed := make(chan error, 1)
	go func() {
		conn, e := listener.listener.Accept(ctx)
		if e != nil {
			failed <- e
		} else {
			accepted <- conn
		}
	}()
	client, e := DialQUIC(ctx, listener.Addr().String(), TLSOptions{Config: credentials.Client}, QUICConfig{}, ClientConfig{ControlTimeout: 20 * time.Millisecond})
	require(t, e)
	t.Cleanup(client.Close)
	var server *quic.Conn
	select {
	case server = <-accepted:
	case e := <-failed:
		t.Fatal(e)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	t.Cleanup(func() { _ = server.CloseWithError(nativeShutdown, "test complete") })
	stream, e := client.conn.OpenStreamSync(ctx)
	require(t, e)
	// Announce the stream. AcceptStream doesn't observe an unwritten stream.
	require(t, stream.TryWriteAll([]byte{0}))
	peer, e := server.AcceptStream(ctx)
	require(t, e)
	var one [1]byte
	_, e = io.ReadFull(peer, one[:])
	require(t, e)
	channel := &nativeChannel{Stream: stream}
	t.Cleanup(func() {
		channel.Close()
		peer.CancelRead(nativeCancel)
		peer.CancelWrite(nativeCancel)
	})
	return channel, peer, client
}
func fillNativeWriteCredit(t testing.TB, ch *nativeChannel) []byte {
	t.Helper()
	fill := bytes.Repeat([]byte{0x5a}, (64<<10)-1)
	require(t, ch.Stream.TryWriteAll(fill))
	if e := ch.Stream.TryWriteAll([]byte{0x33}); !errors.Is(e, quic.ErrWouldBlock) {
		t.Fatal("test did not fill native stream credit", e)
	}
	return fill
}
func timeoutError(t testing.TB, e error) {
	t.Helper()
	var ne net.Error
	if !errors.Is(e, os.ErrDeadlineExceeded) || !errors.As(e, &ne) || !ne.Timeout() {
		t.Fatal("write deadline identity lost", e)
	}
}
func TestNativeWriteExpiredDeadlineAndClear(t *testing.T) {
	for _, both := range []bool{false, true} {
		name := "write"
		if both {
			name = "both"
		}
		t.Run(name, func(t *testing.T) {
			ch, peer, _ := nativeWritePair(t)
			expired := time.Now().Add(-time.Second)
			if both {
				require(t, ch.SetDeadline(expired))
			} else {
				require(t, ch.SetWriteDeadline(expired))
			}
			n, e := ch.Write([]byte("must not be queued"))
			if n != 0 {
				t.Fatal("expired write accepted bytes", n, e)
			}
			timeoutError(t, e)
			require(t, peer.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
			var b [32]byte
			n, e = peer.Read(b[:])
			if n != 0 {
				t.Fatal("expired write reached peer", n)
			}
			timeoutError(t, e)
			require(t, ch.SetWriteDeadline(time.Time{}))
			require(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
			n, e = ch.Write([]byte("after clear"))
			require(t, e)
			if n != 11 {
				t.Fatal(n)
			}
			got := make([]byte, 11)
			_, e = io.ReadFull(peer, got)
			require(t, e)
			if string(got) != "after clear" {
				t.Fatal(string(got))
			}
		})
	}
}
func TestNativeWriteCreditFallbackExactlyOnce(t *testing.T) {
	ch, peer, _ := nativeWritePair(t)
	fill := fillNativeWriteCredit(t, ch)
	payload := bytes.Repeat([]byte{0xa7}, 512)
	require(t, ch.SetWriteDeadline(time.Now().Add(2*time.Second)))
	done := make(chan error, 1)
	started := make(chan struct{})
	go func() {
		close(started)
		n, e := ch.Write(payload)
		if e == nil && n != len(payload) {
			e = io.ErrShortWrite
		}
		done <- e
	}()
	<-started
	select {
	case e := <-done:
		t.Fatal("write completed without peer credit", e)
	case <-time.After(20 * time.Millisecond):
	}
	require(t, peer.SetReadDeadline(time.Now().Add(2*time.Second)))
	got := make([]byte, len(fill)+len(payload))
	_, e := io.ReadFull(peer, got)
	require(t, e)
	require(t, <-done)
	if !bytes.Equal(got[:len(fill)], fill) || !bytes.Equal(got[len(fill):], payload) {
		t.Fatal("fallback duplicated or corrupted prefix")
	}
	require(t, peer.SetReadDeadline(time.Now().Add(20*time.Millisecond)))
	var extra [1]byte
	n, e := peer.Read(extra[:])
	if n != 0 {
		t.Fatal("fallback duplicated bytes")
	}
	timeoutError(t, e)
}
func TestNativeWriteCreditDeadlineThenReuse(t *testing.T) {
	ch, peer, _ := nativeWritePair(t)
	fill := fillNativeWriteCredit(t, ch)
	require(t, ch.SetWriteDeadline(time.Now().Add(25*time.Millisecond)))
	n, e := ch.Write(bytes.Repeat([]byte{0x77}, 512))
	if n != 0 {
		t.Fatal("unexpected credit", n)
	}
	timeoutError(t, e)
	require(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	got := make([]byte, len(fill))
	_, e = io.ReadFull(peer, got)
	require(t, e)
	if !bytes.Equal(got, fill) {
		t.Fatal("fill corrupted")
	}
	require(t, ch.SetWriteDeadline(time.Time{}))
	n, e = ch.Write([]byte("next"))
	require(t, e)
	if n != 4 {
		t.Fatal(n)
	}
	var next [4]byte
	_, e = io.ReadFull(peer, next[:])
	require(t, e)
	if string(next[:]) != "next" {
		t.Fatal("timed out bytes were queued", next)
	}
}
func TestNativeWriteFallbackCancellationReleasesPoolBytes(t *testing.T) {
	ch, _, _ := nativeWritePair(t)
	fillNativeWriteCredit(t, ch)
	p, rs, _ := poolFixture(t, 1)
	cfg, e := normalizeIO(IOConfig{MaxFrameBytes: 1024})
	require(t, e)
	l := newLink(ch, cfg)
	l.resources = rs[0]
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, e := l.send(ctx, wire.Frame{Kind: wire.Data, ID: 1, Number: 1, Payload: bytes.Repeat([]byte{'x'}, 512)})
		done <- e
	}()
	eventually(t, func() bool { return p.Stats().Memory.Send.Used == 536 })
	cancel()
	select {
	case e = <-done:
		if !errors.Is(e, context.Canceled) {
			t.Fatal(e)
		}
	case <-time.After(time.Second):
		t.Fatal("deadline update blocked by fallback write")
	}
	if s := p.Stats(); s.Total.Used != 0 || s.Active.Leases != 0 {
		t.Fatal(s)
	}
}
func TestNativeWriteConcurrentDeadlineUpdates(t *testing.T) {
	ch, peer, _ := nativeWritePair(t)
	const records = 512
	read := make(chan error, 1)
	go func() { _, e := io.CopyN(io.Discard, peer, records*16); read <- e }()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < records; j++ {
				if e := ch.SetWriteDeadline(time.Now().Add(3 * time.Second)); e != nil {
					t.Error(e)
				}
			}
		}()
	}
	for i := 0; i < records; i++ {
		n, e := ch.Write(bytes.Repeat([]byte{byte(i)}, 16))
		require(t, e)
		if n != 16 {
			t.Fatal(n)
		}
	}
	wg.Wait()
	require(t, <-read)
	require(t, ch.SetWriteDeadline(time.Now().Add(-time.Second)))
	n, e := ch.Write([]byte{1})
	if n != 0 {
		t.Fatal(n)
	}
	timeoutError(t, e)
}
func TestNativeWriteBoundedBurstAndEmptyWrite(t *testing.T) {
	ch, peer, _ := nativeWritePair(t)
	require(t, ch.SetWriteDeadline(time.Now().Add(2*time.Second)))
	want := []byte{}
	for i := 0; i < 4; i++ {
		payload := bytes.Repeat([]byte{byte(i + 1)}, 1024)
		n, e := ch.Write(payload)
		require(t, e)
		if n != len(payload) {
			t.Fatal(n)
		}
		want = append(want, payload...)
	}
	if ch.queuedBytes != nativeAtomicWriteBytes {
		t.Fatal("burst not bounded", ch.queuedBytes)
	}
	n, e := ch.Write(nil)
	require(t, e)
	if n != 0 || ch.queuedBytes != nativeAtomicWriteBytes {
		t.Fatal("empty write forged flush credit", n, ch.queuedBytes)
	}
	n, e = ch.Write([]byte{0xee})
	require(t, e)
	if n != 1 || ch.queuedBytes != 0 {
		t.Fatal("paced write failed to drain prefix", n, ch.queuedBytes)
	}
	want = append(want, 0xee)
	require(t, peer.SetReadDeadline(time.Now().Add(time.Second)))
	got := make([]byte, len(want))
	_, e = io.ReadFull(peer, got)
	require(t, e)
	if !bytes.Equal(got, want) {
		t.Fatal("burst corrupted")
	}
	n, e = ch.Write([]byte{0xab})
	require(t, e)
	if n != 1 || ch.queuedBytes != 1 {
		t.Fatal("new bounded burst not started", n, ch.queuedBytes)
	}
}
