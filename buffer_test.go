package stream

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatPath(t *testing.T) {
	assert.Equal(t, "1-1", formatPath(1, 1))
}

func BenchmarkFormatPath(b *testing.B) {
	b.SetParallelism(10)
	for i := 0; i < b.N; i++ {
		_ = formatPath(1024, 1024)
	}
}
