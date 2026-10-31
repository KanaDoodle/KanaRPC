package kanarpc

import (
	"errors"
	"time"

	"github.com/KanaDoodle/KanaRPC/internal/wire"
)

type IOConfig struct {
	// MaxFrameBytes bounds an entire payload, including Open metadata.
	MaxFrameBytes uint32
	WriteTimeout  time.Duration
	// ReadTimeout bounds the time to read one complete record, including idle.
	ReadTimeout time.Duration
}
type ClientConfig struct {
	Feedback       *FeedbackConfig
	Memory         MemoryConfig
	IO             IOConfig
	Timeout        time.Duration
	MaxInFlight    int
	StreamWindow   int
	ControlTimeout time.Duration
}
type ServerConfig struct {
	Governance     *GovernanceConfig
	Memory         MemoryConfig
	IO             IOConfig
	MaxConnections int
	// MaxInFlight includes queued work, handlers and terminal-ACK/FIN waiters.
	MaxInFlight        int
	MaxPerConnection   int
	MaxRequestDuration time.Duration
	AckTimeout         time.Duration
}

func normalizeIO(c IOConfig) (IOConfig, error) {
	if c.MaxFrameBytes == 0 {
		c.MaxFrameBytes = 1 << 20
	}
	if c.WriteTimeout == 0 {
		c.WriteTimeout = time.Second
	}
	if c.ReadTimeout == 0 {
		c.ReadTimeout = 30 * time.Second
	}
	if c.MaxFrameBytes < 1024 || c.MaxFrameBytes > wire.HardLimit || c.WriteTimeout <= 0 || c.ReadTimeout <= 0 {
		return c, errors.New("kanarpc: invalid IO bounds")
	}
	return c, nil
}
func normalizeClient(c ClientConfig) (ClientConfig, error) {
	var err error
	if c.Feedback, err = normalizeFeedback(c.Feedback); err != nil {
		return c, err
	}
	if c.IO, err = normalizeIO(c.IO); err != nil {
		return c, err
	}
	if c.Memory.EncodingBytes == 0 && int64(c.IO.MaxFrameBytes)*2 > 16<<20 {
		c.Memory.EncodingBytes = int64(c.IO.MaxFrameBytes) * 2
	}
	if c.Memory, err = memoryConfig(c.Memory); err != nil {
		return c, err
	}
	if c.Memory.EncodingBytes < int64(c.IO.MaxFrameBytes)*2 {
		return c, errors.New("kanarpc: encoding capacity must hold two maximum frames")
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	if c.MaxInFlight == 0 {
		c.MaxInFlight = 64
	}
	if c.StreamWindow == 0 {
		c.StreamWindow = 8
	}
	if c.ControlTimeout == 0 {
		c.ControlTimeout = 250 * time.Millisecond
	}
	if c.Timeout <= 0 || c.MaxInFlight < 1 || c.MaxInFlight > 4096 || c.StreamWindow < 1 || c.StreamWindow > 32 || c.ControlTimeout <= 0 {
		return c, errors.New("kanarpc: invalid client bounds")
	}
	return c, nil
}
func normalizeServer(c ServerConfig) (ServerConfig, error) {
	var err error
	if c.Memory, err = memoryConfig(c.Memory); err != nil {
		return c, err
	}
	if c.IO, err = normalizeIO(c.IO); err != nil {
		return c, err
	}
	if c.MaxConnections == 0 {
		c.MaxConnections = 64
	}
	if c.MaxInFlight == 0 {
		c.MaxInFlight = 128
	}
	if c.MaxPerConnection == 0 {
		c.MaxPerConnection = 32
	}
	if c.MaxRequestDuration == 0 {
		c.MaxRequestDuration = 30 * time.Second
	}
	if c.AckTimeout == 0 {
		c.AckTimeout = time.Second
	}
	if c.MaxConnections < 1 || c.MaxConnections > 4096 || c.MaxInFlight < 1 || c.MaxInFlight > 4096 || c.MaxPerConnection < 1 || c.MaxPerConnection > 4096 || c.MaxRequestDuration <= 0 || c.AckTimeout <= 0 {
		return c, errors.New("kanarpc: invalid server bounds")
	}
	if c.Governance, err = normalizeGovernance(c.Governance, c.MaxInFlight); err != nil {
		return c, err
	}
	return c, nil
}
