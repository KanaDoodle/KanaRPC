package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

type fragmentReader struct{ *bytes.Reader }

func (r fragmentReader) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return r.Reader.Read(p)
}

type zeroWriter struct{}

type fragmentWriter struct{ bytes.Buffer }

func (w *fragmentWriter) Write(b []byte) (int, error) {
	if len(b) > 1 {
		b = b[:1]
	}
	return w.Buffer.Write(b)
}
func TestPartialRecordWritesPreserveWireBytes(t *testing.T) {
	var output fragmentWriter
	want := Frame{Kind: Data, ID: 17, Number: 4, Payload: []byte(`{"text":"fragmented"}`)}
	if err := Write(&output, want, 1024); err != nil {
		t.Fatal(err)
	}
	got, err := Read(&output.Buffer, 1024)
	if err != nil || got.Kind != want.Kind || got.ID != want.ID || got.Number != want.Number || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatal(got, err)
	}
}

func (zeroWriter) Write([]byte) (int, error) { return 0, nil }

func TestRecordsSurviveFragmentationAndConcatenation(t *testing.T) {
	frames := []Frame{{Kind: Open, ID: 1, Payload: []byte(`{"request":1}`)}, {Kind: Data, ID: 1, Number: 1, Payload: []byte("1")}, {Kind: Credit, ID: 1, Number: 1}, {Kind: Cancel, ID: 1}, {Kind: End, ID: 1, Payload: []byte(`{"code":"ok"}`)}, {Kind: Ack, ID: 1, Payload: make([]byte, 16)}}
	var b bytes.Buffer
	for _, f := range frames {
		if err := Write(&b, f, 1024); err != nil {
			t.Fatal(err)
		}
	}
	r := fragmentReader{bytes.NewReader(b.Bytes())}
	for _, want := range frames {
		got, err := Read(r, 1024)
		if err != nil || got.Kind != want.Kind || got.ID != want.ID || got.Number != want.Number || !bytes.Equal(got.Payload, want.Payload) {
			t.Fatal(got, err)
		}
	}
	if _, err := Read(r, 1024); err != io.EOF {
		t.Fatal(err)
	}
}
func TestMalformedHeadersRejectedBeforePayloadAllocation(t *testing.T) {
	var b bytes.Buffer
	_ = Write(&b, Frame{Kind: Open, ID: 1, Payload: []byte("1")}, 1024)
	base := b.Bytes()
	for _, mutate := range []func([]byte){func(b []byte) { b[0] = '?' }, func(b []byte) { b[4] = 1 }, func(b []byte) { b[5] = 255 }, func(b []byte) { b[6] = 1 }, func(b []byte) { clear(b[8:16]) }, func(b []byte) { binary.BigEndian.PutUint32(b[16:20], HardLimit+1) }, func(b []byte) { binary.BigEndian.PutUint32(b[20:24], 1) }} {
		bad := bytes.Clone(base)
		mutate(bad)
		if _, err := Read(bytes.NewReader(bad), 1024); !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
	}
}
func TestInvalidWritesAndZeroProgress(t *testing.T) {
	for _, f := range []Frame{{Kind: Open, ID: 0, Payload: []byte("1")}, {Kind: Credit, ID: 1, Number: 2}, {Kind: Ack, ID: 1, Payload: []byte("short")}, {Kind: Cancel, ID: 1, Payload: []byte("x")}} {
		if err := Write(io.Discard, f, 1024); !errors.Is(err, ErrProtocol) {
			t.Fatal(err)
		}
	}
	if err := Write(zeroWriter{}, Frame{Kind: Open, ID: 1, Payload: []byte("1")}, 1024); !errors.Is(err, io.ErrNoProgress) {
		t.Fatal(err)
	}
}
func FuzzRead(f *testing.F) {
	var b bytes.Buffer
	_ = Write(&b, Frame{Kind: Data, ID: 7, Number: 1, Payload: []byte("42")}, 1024)
	f.Add(b.Bytes())
	f.Add([]byte("KNR2"))
	f.Add(make([]byte, HeaderSize))
	f.Fuzz(func(t *testing.T, b []byte) {
		frame, err := Read(bytes.NewReader(b), 1024)
		if err != nil {
			return
		}
		if len(frame.Payload) > 1024 || !valid(frame, uint32(len(frame.Payload))) {
			t.Fatal("bounds violated")
		}
		var roundtrip bytes.Buffer
		if err := Write(&roundtrip, frame, 1024); err != nil {
			t.Fatal(err)
		}
		if _, err := Read(&roundtrip, 1024); err != nil {
			t.Fatal(err)
		}
	})
}

func FuzzReadReserved(f *testing.F) {
	var valid bytes.Buffer
	_ = Write(&valid, Frame{Kind: Data, ID: 1, Number: 1, Payload: []byte("42")}, 1024)
	f.Add(valid.Bytes())
	f.Add(valid.Bytes()[:HeaderSize])
	f.Add([]byte("KNR2"))
	f.Fuzz(func(t *testing.T, input []byte) {
		if len(input) > 8192 {
			return
		}
		used, released, accepted := uint32(0), 0, false
		frame, err := ReadReserved(bytes.NewReader(input), HardLimit, func(_ Kind, n uint32) (func(), error) {
			if n > 1024 {
				return nil, errors.New("fixture budget")
			}
			used += n
			accepted = true
			return func() { used -= n; released++ }, nil
		})
		if err != nil && used != 0 {
			t.Fatal("failed read retained reservation", used)
		}
		copy := frame
		frame.Release()
		copy.Release()
		if used != 0 || accepted && released != 1 {
			t.Fatal("reservation not released exactly once", used, released)
		}
	})
}
