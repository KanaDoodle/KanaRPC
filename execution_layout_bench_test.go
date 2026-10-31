package kanarpc

import (
	"testing"
	"unsafe"
)

var executionLayoutSink *execution

func BenchmarkExecutionAllocation(b *testing.B) {
	b.ReportAllocs()
	b.ReportMetric(float64(unsafe.Sizeof(execution{})), "execution-bytes")
	for i := 0; i < b.N; i++ {
		executionLayoutSink = new(execution)
	}
}
