package stream

import "github.com/Xuanwo/go-bufferpool"

var pool = bufferpool.New(64)

func formatPath(id, idx uint64) string {
	buf := pool.Get()
	defer buf.Free()

	buf.AppendUint(id)
	buf.AppendByte('-')
	buf.AppendUint(idx)
	return string(buf.Bytes())
}
