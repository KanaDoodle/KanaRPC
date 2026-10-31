package kanarpc

import "sync/atomic"

// CompressionStats measures prepared business bodies, not successful delivery,
// TCP/TLS/QUIC overhead, or application consumption. Counters are cumulative.
type CompressionStats struct{ Prepared, GzipPrepared, PlainBytes, EncodedBodyBytes, DecodedGzip, InflatedBytes uint64 }
type compressionCounters struct{ prepared, gzip, plain, encoded, decoded, inflated atomic.Uint64 }

func (c *compressionCounters) encode(plain, encoded int, gzip bool) {
	c.prepared.Add(1)
	c.plain.Add(uint64(plain))
	c.encoded.Add(uint64(encoded))
	if gzip {
		c.gzip.Add(1)
	}
}
func (c *compressionCounters) decode(plain uint32) { c.decoded.Add(1); c.inflated.Add(uint64(plain)) }
func (c *compressionCounters) snapshot() CompressionStats {
	return CompressionStats{c.prepared.Load(), c.gzip.Load(), c.plain.Load(), c.encoded.Load(), c.decoded.Load(), c.inflated.Load()}
}
func (c *Client) CompressionStats() CompressionStats     { return c.resources.compression.snapshot() }
func (c *QUICClient) CompressionStats() CompressionStats { return c.resources.compression.snapshot() }
func (s *Server) CompressionStats() CompressionStats     { return s.resources.compression.snapshot() }
