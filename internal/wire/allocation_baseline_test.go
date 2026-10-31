package wire

import (
	"encoding/binary"
	"io"
	"sync"
	"testing"
)

type claimedReader struct {
	header  []byte
	entered chan<- int
	release <-chan struct{}
	once    sync.Once
}

func (r *claimedReader) Read(b []byte) (int, error) {
	if len(r.header) > 0 {
		n := copy(b, r.header)
		r.header = r.header[n:]
		return n, nil
	}
	r.once.Do(func() { r.entered <- len(b) })
	<-r.release
	return 0, io.EOF
}
func TestHeaderClaimsAllocationBaseline(t *testing.T) {
	const streams = 4
	const claim = 1 << 20
	entered, release := make(chan int, streams), make(chan struct{})
	var group sync.WaitGroup
	for i := 0; i < streams; i++ {
		header := make([]byte, HeaderSize)
		copy(header, "KNR2")
		header[4] = 2
		header[5] = byte(Open)
		binary.BigEndian.PutUint64(header[8:16], 1)
		binary.BigEndian.PutUint32(header[16:20], claim)
		reader := &claimedReader{header: header, entered: entered, release: release}
		group.Add(1)
		go func() { defer group.Done(); _, _ = Read(reader, claim) }()
	}
	total := 0
	for i := 0; i < streams; i++ {
		total += <-entered
	}
	close(release)
	group.Wait()
	if total != streams*claim {
		t.Fatal(total)
	}
	t.Logf("unbudgeted reference reader offered %d allocated payload bytes after headers, before any payload arrived; not RSS", total)
}
