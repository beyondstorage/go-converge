package stream

import (
	"fmt"
	"golang.org/x/time/rate"
	"io"
	"sync"

	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/panjf2000/ants/v2"
	"go.uber.org/atomic"
)

type Stream struct {
	method string
	upper  types.Storager
	under  types.Storager
	// Only valid if method is "multipart" and under supports.
	underMultipart types.Multiparter
	// Only valid if method is "append" and under supports.
	underAppend types.Appender

	limit *rate.Limiter
	p     *ants.Pool
	ch    chan op
	errch chan error
}

type op struct {
	br   *Branch
	size int64
	done bool
}

func (s *Stream) StartBranch(id uint64, path string) (br *Branch, err error) {
	br = &Branch{
		s:    s,
		wg:   &sync.WaitGroup{},
		id:   id,
		path: path,

		nextIdx:     atomic.NewUint64(0),
		currentSize: atomic.NewInt64(0),
	}

	switch s.method {
	case PersistMethodMultipart:
		o, err := s.underMultipart.CreateMultipart(br.path)
		if err != nil {
			return nil, err
		}
		br.object = o
		br.parts = make(map[int]*types.Part)
	default:
		panic(fmt.Errorf("start Branch with invalid method: %v", s.method))
	}

	return br, nil
}

func (s *Stream) Errors() chan error {
	return s.errch
}

func (s *Stream) Serve() {
	for op := range s.ch {
		op.br.persist(op.size, op.done)
	}
}

func (s *Stream) read(id, start, end uint64) (r io.ReadCloser, err error) {
	r, w := io.Pipe()

	go func() {
		for i := start; i < end; i++ {
			p := formatPath(id, i)
			_, err := s.upper.Read(p, w)
			if err != nil {
				s.errch <- fmt.Errorf("pipe read from %s: %w", p, err)
				return
			}
		}
		err := w.Close()
		if err != nil {
			s.errch <- fmt.Errorf("close pipe writter: %w", err)
			return
		}
	}()

	return r, nil
}

func (s *Stream) delete(id, start, end uint64) (err error) {
	for i := start; i < end; i++ {
		p := formatPath(id, i)
		err = s.upper.Delete(p)
		if err != nil {
			return
		}
	}
	return
}
