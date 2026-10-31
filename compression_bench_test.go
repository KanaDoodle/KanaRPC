package kanarpc

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
)

func BenchmarkGzipBody(b *testing.B) {
	for _, size := range []int{64, 4096, 65536} {
		for _, random := range []bool{false, true} {
			b.Run(fmt.Sprintf("bytes%d/random%v", size, random), func(b *testing.B) {
				body := make([]byte, size)
				if random {
					_, _ = rand.New(rand.NewSource(7)).Read(body)
				} else {
					for i := range body {
						body[i] = 'a'
					}
				}
				b.ReportAllocs()
				b.SetBytes(int64(size))
				b.ResetTimer()
				wireBytes := 0
				for i := 0; i < b.N; i++ {
					out, _, err := gzipBody(context.Background(), body, 1024)
					if err != nil {
						b.Fatal(err)
					}
					wireBytes = len(out)
				}
				b.StopTimer()
				b.ReportMetric(float64(wireBytes)/float64(size), "body_ratio")
			})
		}
	}
}
