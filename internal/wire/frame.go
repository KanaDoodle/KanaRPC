// Package wire implements the experimental KanaRPC v2 record format.
package wire

import (
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
)

const HeaderSize = 24
const HardLimit = 16 << 20

type Kind uint8

const (
	Open Kind = iota + 1
	Data
	End
	Cancel
	Credit
	Ack
	SessionBarrier
	OpenEncoded
	DataEncoded
	Hello
	Welcome
	OpenOptions
	DataOptions
)

var ErrProtocol = errors.New("kanarpc: invalid v2 record")

type Frame struct {
	Kind    Kind
	ID      uint64
	Number  uint32
	Payload []byte
	lease   *reservation
}

type reservation struct {
	once    sync.Once
	release func()
}

func (f *Frame) Release() {
	if f.lease != nil {
		f.lease.once.Do(f.lease.release)
	}
	f.Payload = nil
}

func valid(f Frame, size uint32) bool {
	if f.ID == 0 || size > HardLimit {
		return false
	}
	switch f.Kind {
	case Open, OpenEncoded, OpenOptions, End:
		return f.Number == 0 && size > 0
	case Data:
		return f.Number > 0 && size > 0
	case DataOptions:
		return f.Number > 0 && size >= 5
	case DataEncoded:
		return f.Number > 0 // An empty Protobuf message has zero encoded bytes.
	case Cancel:
		return f.Number == 0 && size == 0
	case Credit:
		return f.Number == 1 && size == 0
	case Ack, SessionBarrier:
		return f.Number == 0 && size == 16
	case Hello, Welcome:
		return f.Number == 0 && size == 32
	}
	return false
}

func Read(r io.Reader, limit uint32) (Frame, error) {
	return ReadReserved(r, limit, nil)
}

// Reserve runs after header validation and before payload allocation.
func ReadReserved(r io.Reader, limit uint32, reserve func(Kind, uint32) (func(), error)) (Frame, error) {
	var h [HeaderSize]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return Frame{}, err
	}
	f := Frame{Kind: Kind(h[5]), ID: binary.BigEndian.Uint64(h[8:16]), Number: binary.BigEndian.Uint32(h[20:24])}
	n := binary.BigEndian.Uint32(h[16:20])
	if string(h[:4]) != "KNR2" || h[4] != 2 || h[6] != 0 || h[7] != 0 || n > limit || !valid(f, n) {
		return Frame{}, ErrProtocol
	}
	if reserve != nil {
		release, err := reserve(f.Kind, n)
		if err != nil {
			return Frame{}, err
		}
		f.lease = &reservation{release: release}
	}
	f.Payload = make([]byte, n)
	_, err := io.ReadFull(r, f.Payload)
	if err != nil {
		f.Release()
	}
	return f, err
}

func Write(w io.Writer, f Frame, limit uint32) error {
	if uint64(len(f.Payload)) > uint64(limit) || !valid(f, uint32(len(f.Payload))) {
		return ErrProtocol
	}
	// Coalesce a record for every transport. Otherwise TLS encrypts the header
	// and payload as separate writes while QUIC buffers both, a weak control.
	packet := make([]byte, HeaderSize+len(f.Payload))
	putHeader(packet[:HeaderSize], f)
	copy(packet[HeaderSize:], f.Payload)
	return writePacket(w, packet)
}

func putHeader(h []byte, f Frame) {
	copy(h[:4], "KNR2")
	h[4], h[5] = 2, byte(f.Kind)
	binary.BigEndian.PutUint64(h[8:16], f.ID)
	binary.BigEndian.PutUint32(h[16:20], uint32(len(f.Payload)))
	binary.BigEndian.PutUint32(h[20:24], f.Number)
}

// WriteTCP avoids a payload-sized coalescing copy on a concrete TCP socket.
// Buffers.WriteTo uses the socket's writev path and handles partial writes.
// TLS and native QUIC retain their existing coalesced-record writes.
func WriteTCP(w *net.TCPConn, f Frame, limit uint32) error {
	if uint64(len(f.Payload)) > uint64(limit) || !valid(f, uint32(len(f.Payload))) {
		return ErrProtocol
	}
	var h [HeaderSize]byte
	putHeader(h[:], f)
	buffers := net.Buffers{h[:], f.Payload}
	n, err := buffers.WriteTo(w)
	if err == nil && n != int64(HeaderSize+len(f.Payload)) {
		return io.ErrShortWrite
	}
	return err
}

// WriteCredits coalesces ordinary CREDIT=1 records without changing the wire
// format. The caller owns a reservation for count*HeaderSize until it returns.
func WriteCredits(w io.Writer, id uint64, count uint32) error {
	if id == 0 || count == 0 || count > 16 {
		return ErrProtocol
	}
	packet := make([]byte, int(count)*HeaderSize)
	for i := uint32(0); i < count; i++ {
		h := packet[int(i)*HeaderSize:][:HeaderSize]
		copy(h[:4], "KNR2")
		h[4], h[5] = 2, byte(Credit)
		binary.BigEndian.PutUint64(h[8:16], id)
		binary.BigEndian.PutUint32(h[20:24], 1)
	}
	return writePacket(w, packet)
}

func writePacket(w io.Writer, packet []byte) error {
	for len(packet) > 0 {
		n, err := w.Write(packet)
		if n < 0 || n > len(packet) {
			return io.ErrShortWrite
		}
		packet = packet[n:]
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrNoProgress
		}
	}
	return nil
}
