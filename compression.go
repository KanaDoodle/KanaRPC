package kanarpc

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

const compressionWriteWorkspace int64 = 2 << 20
const compressionReadWorkspace int64 = 128 << 10

// CompressionConfig enables gzip negotiation. Small/unprofitable messages stay
// plain. Workspace reservations are additional to payload reservations.
type CompressionConfig struct{ MinBytes int }

func compressionConfig(c *CompressionConfig) (*CompressionConfig, error) {
	if c == nil {
		return nil, nil
	}
	v := *c
	if v.MinBytes == 0 {
		v.MinBytes = 1024
	}
	if v.MinBytes < 1 || v.MinBytes > wire.HardLimit {
		return nil, errors.New("kanarpc: invalid compression threshold")
	}
	return &v, nil
}

type cappedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if len(p) > b.limit-b.Len() {
		return 0, io.ErrShortBuffer
	}
	return b.Buffer.Write(p)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := deadlineError(r.ctx); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
func gzipBody(ctx context.Context, b []byte, min int) ([]byte, bool, error) {
	if len(b) < min {
		return b, false, nil
	}
	if err := deadlineError(ctx); err != nil {
		return nil, false, err
	}
	// Exact capacity is bounded by the original body; gzip headers alone can make
	// tiny or incompressible messages fall back without allocating an expanded body.
	dst := &cappedBuffer{limit: len(b)}
	dst.Grow(len(b))
	z, err := gzip.NewWriterLevel(dst, gzip.BestSpeed)
	if err != nil {
		return nil, false, err
	}
	_, err = io.Copy(z, contextReader{ctx, bytes.NewReader(b)})
	closeErr := z.Close()
	if errors.Is(err, io.ErrShortBuffer) || errors.Is(closeErr, io.ErrShortBuffer) {
		return b, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	if closeErr != nil {
		return nil, false, closeErr
	}
	if dst.Len() >= len(b) {
		return b, false, nil
	}
	return dst.Bytes(), true, nil
}
func inflateBody(ctx context.Context, b []byte, n uint32) ([]byte, error) {
	if e := deadlineError(ctx); e != nil {
		return nil, e
	}
	source := bytes.NewReader(b)
	z, err := gzip.NewReader(source)
	if err != nil {
		return nil, ErrProtocol
	}
	defer z.Close()
	z.Multistream(false)
	out := make([]byte, int(n))
	_, err = io.ReadFull(contextReader{ctx, z}, out)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, ErrProtocol
	}
	var extra [1]byte
	count, e := z.Read(extra[:])
	if count != 0 || e != io.EOF || source.Len() != 0 {
		return nil, ErrProtocol
	}
	return out, nil
}
func decodeOptionsOpen(f wire.Frame) (openRecord, payloadEncoding, error) {
	var r openRecord
	if len(f.Payload) < 4 {
		return r, encodingJSON, ErrProtocol
	}
	n := binary.BigEndian.Uint32(f.Payload[:4])
	if n == 0 || n > 8192 || uint64(n)+4 > uint64(len(f.Payload)) {
		return r, encodingJSON, ErrProtocol
	}
	if json.Unmarshal(f.Payload[4:4+n], &r) != nil || r.Request != nil || validateMetadata(r.Metadata) != nil {
		return r, encodingJSON, ErrProtocol
	}
	encoding := encodingJSON
	if r.Codec == "protobuf" {
		encoding = encodingProtobuf
	} else if r.Codec != "json" {
		return r, encoding, ErrProtocol
	}
	if r.Compression != "" && r.Compression != "gzip" {
		return r, encoding, ErrProtocol
	}
	r.Request = f.Payload[4+n:]
	if r.Compression == "" && len(r.Request) != int(r.PlainBytes) {
		return r, encoding, ErrProtocol
	}
	return r, encoding, nil
}
func optionsMetadata(p *preparedCall, budget int64) ([]byte, error) {
	codec := "json"
	if p.encoding == encodingProtobuf {
		codec = "protobuf"
	}
	r := openRecord{Method: p.method, BudgetNanos: budget, Window: p.window, Stream: p.streaming, Codec: codec, Metadata: p.metadata, PlainBytes: p.plainBytes, AcceptGzip: p.acceptGzip}
	if p.compressed {
		r.Compression = "gzip"
	}
	meta, e := json.Marshal(r)
	if e != nil {
		return nil, e
	}
	if len(meta) > 8192 {
		return nil, &Status{Code: InvalidArgument, Message: "metadata exceeds envelope limit"}
	}
	return meta, nil
}
func optionsOpen(p *preparedCall, budget int64) ([]byte, error) {
	meta, e := optionsMetadata(p, budget)
	if e != nil {
		return nil, e
	}
	out := make([]byte, 4+len(meta)+len(p.body))
	binary.BigEndian.PutUint32(out, uint32(len(meta)))
	copy(out[4:], meta)
	copy(out[4+len(meta):], p.body)
	return out, nil
}
