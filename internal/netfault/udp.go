// Package netfault is an owned single-client UDP test relay. It never terminates
// QUIC/TLS and is not a production proxy or a substitute for kernel netem.
package netfault

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"
)

type Direction int

const (
	ToServer Direction = iota
	ToClient
)

type Plan struct {
	Delay               time.Duration
	BytesPerSecond      int64
	DropNext, DropEvery int
	Blackhole           bool
}
type Counters struct {
	Received, Forwarded, Dropped, Foreign, WriteErrors            uint64
	BytesReceived, BytesForwarded, PendingBytes, PeakPendingBytes int64
}
type Stats struct {
	ToServer, ToClient Counters
	Generation         uint64
	Closed             bool
}
type UDP struct {
	front, back  *net.UDPConn
	remote, peer *net.UDPAddr
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.Mutex
	plans        [2]Plan
	counters     [2]Counters
	generation   uint64
	closed       bool
	wg           sync.WaitGroup
	once         sync.Once
}

func NewUDP(target string) (*UDP, error) {
	remote, err := net.ResolveUDPAddr("udp4", target)
	if err != nil {
		return nil, err
	}
	if remote.IP.To4() == nil {
		return nil, errors.New("netfault: explicit IPv4 target required")
	}
	front, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		return nil, err
	}
	back, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		_ = front.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	r := &UDP{front: front, back: back, remote: remote, ctx: ctx, cancel: cancel}
	r.wg.Add(2)
	go r.pump(ToServer)
	go r.pump(ToClient)
	return r, nil
}
func (r *UDP) Address() string { return r.front.LocalAddr().String() }
func (r *UDP) Set(direction Direction, plan Plan) error {
	if direction < ToServer || direction > ToClient || plan.Delay < 0 || plan.Delay > 2*time.Second || plan.BytesPerSecond < 0 || plan.DropNext < 0 || plan.DropEvery < 0 {
		return errors.New("netfault: invalid bounded plan")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return net.ErrClosed
	}
	r.plans[direction] = plan
	r.generation++
	return nil
}
func (r *UDP) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return Stats{r.counters[0], r.counters[1], r.generation, r.closed}
}
func (r *UDP) Close() {
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		r.mu.Unlock()
		r.cancel()
		_ = r.front.Close()
		_ = r.back.Close()
		r.wg.Wait()
	})
}
func (r *UDP) wait(delay time.Duration) bool {
	if delay <= 0 {
		return r.ctx.Err() == nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-r.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func (r *UDP) clear(direction Direction) {
	r.mu.Lock()
	r.counters[direction].PendingBytes = 0
	r.mu.Unlock()
}
func (r *UDP) pump(direction Direction) {
	defer r.wg.Done()
	defer r.clear(direction)
	source, destination := r.front, r.back
	if direction == ToClient {
		source, destination = r.back, r.front
	}
	// One buffer and one forwarding goroutine per direction; no per-packet task
	// or unbounded application queue. UDP/kernel buffering remains outside this.
	var body [65536]byte
	for {
		n, from, err := source.ReadFromUDP(body[:])
		if err != nil {
			return
		}
		r.mu.Lock()
		counter := &r.counters[direction]
		counter.Received++
		counter.BytesReceived += int64(n)
		valid := true
		var target *net.UDPAddr
		if direction == ToServer {
			if r.peer == nil {
				r.peer = from
			}
			valid = from.String() == r.peer.String()
			target = r.remote
		} else {
			valid = from.String() == r.remote.String()
			target = r.peer
		}
		if !valid || target == nil {
			counter.Foreign++
			r.mu.Unlock()
			continue
		}
		plan := r.plans[direction]
		drop := plan.Blackhole || plan.DropNext > 0 || plan.DropEvery > 0 && counter.Received%uint64(plan.DropEvery) == 0
		if plan.DropNext > 0 {
			r.plans[direction].DropNext--
		}
		if drop {
			counter.Dropped++
			r.mu.Unlock()
			continue
		}
		counter.PendingBytes = int64(n)
		if counter.PendingBytes > counter.PeakPendingBytes {
			counter.PeakPendingBytes = counter.PendingBytes
		}
		r.mu.Unlock()
		delay := plan.Delay
		if plan.BytesPerSecond > 0 {
			delay += time.Duration(int64(n) * int64(time.Second) / plan.BytesPerSecond)
		}
		if !r.wait(delay) {
			return
		}
		r.mu.Lock()
		drop = r.plans[direction].Blackhole
		if drop {
			r.counters[direction].Dropped++
			r.counters[direction].PendingBytes = 0
		}
		r.mu.Unlock()
		if drop {
			continue
		}
		_, err = destination.WriteToUDP(body[:n], target)
		r.mu.Lock()
		counter = &r.counters[direction]
		counter.PendingBytes = 0
		if err != nil {
			counter.WriteErrors++
		} else {
			counter.Forwarded++
			counter.BytesForwarded += int64(n)
		}
		r.mu.Unlock()
	}
}
