package kanarpc

import (
	"bytes"
	"io"
	"testing"
	"time"
)

func FuzzNativeQueuedWrites(f *testing.F) {
	f.Add([]byte{1, 7, 3, 11, 0, 23, 5})
	f.Add([]byte{0, 0, 0})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 32 {
			return
		}
		ch, peer, _ := nativeWritePair(t)
		type result struct {
			body []byte
			err  error
		}
		read := make(chan result, 1)
		require(t, peer.SetReadDeadline(time.Now().Add(3*time.Second)))
		go func() { body, e := io.ReadAll(peer); read <- result{body, e} }()
		var want []byte
		for _, op := range ops {
			payload := bytes.Repeat([]byte{op}, []int{0, 17, 512, 1024, 4096, 8192}[int(op>>2)%6])
			if op%5 == 0 {
				require(t, ch.SetWriteDeadline(time.Now().Add(-time.Second)))
				n, e := ch.Write(payload)
				if n != 0 {
					t.Fatal("expired write accepted", n)
				}
				timeoutError(t, e)
			} else {
				require(t, ch.SetWriteDeadline(time.Now().Add(3*time.Second)))
				n, e := ch.Write(payload)
				require(t, e)
				if n != len(payload) {
					t.Fatal(n, len(payload))
				}
				want = append(want, payload...)
			}
			if ch.queuedBytes < 0 || ch.queuedBytes > nativeAtomicWriteBytes {
				t.Fatal("atomic burst overcommitted", ch.queuedBytes)
			}
		}
		ch.Finish()
		got := <-read
		require(t, got.err)
		if !bytes.Equal(got.body, want) {
			t.Fatal("queued write duplicated, lost or corrupted bytes", len(got.body), len(want))
		}
	})
}
