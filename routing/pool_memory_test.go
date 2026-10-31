package routing

import (
	"context"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"

	rpc "github.com/KanaDoodle/KanaRPC"
	"github.com/KanaDoodle/KanaRPC/internal/testcert"
)

// Compatibility setter lets the identical reproduction compile on R8D, whose
// routing.Config has no pool Memory field. Only the test uses reflection.
func testPoolMemory(cfg *Config, encoding, receive, send, total int64) bool {
	field := reflect.ValueOf(cfg).Elem().FieldByName("Memory")
	if !field.IsValid() {
		return false
	}
	field.FieldByName("DataBytes").SetInt(total)
	limits := field.FieldByName("Memory")
	limits.FieldByName("EncodingBytes").SetInt(encoding)
	limits.FieldByName("ReceiveBytes").SetInt(receive)
	limits.FieldByName("SendBytes").SetInt(send)
	return true
}

type poolEncoder struct {
	count   *atomic.Int64
	entered chan struct{}
	release <-chan struct{}
}

func (e poolEncoder) MarshalJSON() ([]byte, error) {
	e.count.Add(1)
	e.entered <- struct{}{}
	<-e.release
	return []byte("1"), nil
}

func TestPoolEncodingAdmissionAcrossEndpoints(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	a, b := serve(t, rpc.TCP, credentials, 10), serve(t, rpc.TCP, credentials, 20)
	a.node.ID = "a"
	b.node.ID = "b"
	cfg := Config{MaxConnections: 2, MaxInFlight: 4, Dial: options(credentials)}
	cfg.Dial.Client.IO.MaxFrameBytes = 1024
	cfg.Dial.Client.Memory.EncodingBytes = 2048
	supported := testPoolMemory(&cfg, 2048, 4096, 4096, 16384)
	c := managed(t, &source{snapshot: fresh(a.node, b.node)}, cfg, rpc.DialEndpoint)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	var count atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { done <- c.Call(ctx, "/test/echo", poolEncoder{&count, entered, release}, new(int)) }()
	}
	receive(t, entered)
	until(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		preparing, settled := 0, 0
		for e := range c.entries {
			if e.caller == nil {
				return false
			}
			a := e.caller.(interface{ AdmissionStats() rpc.AdmissionStats }).AdmissionStats()
			preparing += a.Preparing
			settled += a.Encoding + a.EncodingWaiters
		}
		return preparing == 2 && settled == 2 && (supported || count.Load() == 2)
	})
	c.mu.Lock()
	var used int64
	for e := range c.entries {
		used += e.caller.(interface{ MemoryStats() rpc.MemoryStats }).MemoryStats().Encoding.Used
	}
	c.mu.Unlock()
	if used > 2048 || count.Load() != 1 {
		t.Errorf("cross-connection encoding exceeded intended pool quota: supported=%v used=%d encoders=%d", supported, used, count.Load())
	}
	cancel()
	once.Do(func() { close(release) })
	receive(t, done)
	receive(t, done)
	if a.executions.Load()+b.executions.Load() != 0 {
		t.Fatal("expired preparation sent business")
	}
}

// This unchanged reproduction also runs on R8D using the compatibility setter.
func TestPoolReceiveAdmissionAcrossCompletedUnreadStreams(t *testing.T) {
	credentials, err := testcert.New()
	check(t, err)
	a, b := serve(t, rpc.TCP, credentials, 1<<60), serve(t, rpc.TCP, credentials, 1<<60)
	a.node.ID = "a"
	b.node.ID = "b"
	cfg := Config{MaxConnections: 2, MaxInFlight: 4, Dial: options(credentials)}
	cfg.Dial.Client.StreamWindow = 32
	cfg.Dial.Client.IO.MaxFrameBytes = 1024
	cfg.Dial.Client.Memory.ReceiveBytes = 1024
	supported := testPoolMemory(&cfg, 4096, 1024, 4096, 16384)
	c := managed(t, &source{snapshot: fresh(a.node, b.node)}, cfg, rpc.DialEndpoint)
	one, err := c.Stream(context.Background(), "/test/items", 32)
	check(t, err)
	defer one.Close()
	until(t, func() bool { return a.s.Stats().InFlight == 0 && a.executions.Load() == 1 })
	two, err := c.Stream(context.Background(), "/test/items", 32)
	check(t, err)
	defer two.Close()
	until(t, func() bool { return b.s.Stats().InFlight == 0 && b.executions.Load() == 1 })
	c.mu.Lock()
	var total int64
	for e := range c.entries {
		total += buffered(e.caller)
	}
	c.mu.Unlock()
	if total > 1024 {
		t.Errorf("unread cross-connection quota exceeded: supported=%v receive=%d quota=1024", supported, total)
	}
	if total == 0 {
		t.Fatal("first stream unexpectedly lost")
	}
	for i := 0; i < 32; i++ {
		var n int
		check(t, one.Recv(&n))
		if n != 1<<60+i {
			t.Fatal(n)
		}
	}
	if e := one.Recv(nil); e != io.EOF {
		t.Fatal(e)
	}
}
