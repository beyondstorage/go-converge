package stream

import (
	"bytes"
	"io"
	"sync"

	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/panjf2000/ants/v2"
)

type Stream struct {
	upper types.Storager
	under types.Storager

	p     *ants.Pool
	ch    chan op
	errch chan error

	branches   map[uint64]*branch
	branchLock sync.Mutex
}

type op struct {
	id   uint64
	size int64
}

type branch struct {
	// branch is not a public struct, it's safe to embed lock directly.
	sync.Mutex
	wg *sync.WaitGroup

	id   uint64
	path string

	// Current branch
	persistedIdx  uint64
	persistedSize int64
	nextIdx       uint64
	currentSize   int64

	// Meta of the object.
	//
	// we can check object mode to decide use CompleteMultipart or call Write.
	object *types.Object

	// Only valid if we have already called CreateMultipart.
	parts          map[int]*types.Part
	nextPartNumber int
}

func newBranch(id uint64, path string) *branch {
	return &branch{
		wg:   &sync.WaitGroup{},
		id:   id,
		path: path,
	}
}

func (s *Stream) StartBranch(id uint64, path string) {
	s.branchLock.Lock()
	defer s.branchLock.Unlock()

	br := newBranch(id, path)

	o, err := s.under.(types.Multiparter).CreateMultipart(br.path)
	if err != nil {
		s.errch <- err
		return
	}

	br.object = o
	br.parts = make(map[int]*types.Part)

	s.branches[id] = br
}

func (s *Stream) WriteBranch(id, idx uint64, data []byte) (n int64, err error) {
	p := formatPath(id, idx)

	size := int64(len(data))
	n, err = s.upper.Write(p, bytes.NewReader(data), size)
	if err != nil {
		return
	}

	s.ch <- op{
		id:   id,
		size: size,
	}
	return n, nil
}

func (s *Stream) EndBranch(id uint64) {
	s.branchLock.Lock()
	br := s.branches[id]
	s.branchLock.Unlock()

	s.complete(br)

	s.branchLock.Lock()
	delete(s.branches, id)
	s.branchLock.Unlock()
	return
}

func (s *Stream) Errors() chan error {
	return s.errch
}

func (s *Stream) Serve() {
	for op := range s.ch {
		s.branchLock.Lock()
		br := s.branches[op.id]
		s.branchLock.Unlock()

		br.Lock()
		br.nextIdx += 1
		br.currentSize += op.size
		br.Unlock()

		// Skip write operation if we don't have enough data.
		// TODO: make we can allow user to configure the buffer size?
		if br.currentSize-br.persistedSize < 4*1024*1024 {
			continue
		}

		br.Lock()
		start := br.persistedIdx
		end := br.nextIdx
		size := br.currentSize - br.persistedSize
		partNumber := br.nextPartNumber
		br.persistedSize = br.currentSize
		br.persistedIdx = br.nextIdx
		br.nextPartNumber += 1
		br.Unlock()

		br.wg.Add(1)
		err := s.p.Submit(func() {
			defer br.wg.Done()

			s.persistViaWriteMultipart(br, start, end, size, partNumber)
		})
		if err != nil {
			s.errch <- err
			return
		}
	}
}

func (s *Stream) persistViaWriteMultipart(br *branch, start, end uint64, size int64, partNumber int) {
	r, err := s.read(br.id, start, end)
	if err != nil {
		s.errch <- err
		return
	}
	defer func() {
		err = r.Close()
		if err != nil {
			s.errch <- err
			return
		}
	}()

	_, part, err := s.under.(types.Multiparter).WriteMultipart(br.object, r, size, partNumber)
	if err != nil {
		s.errch <- err
		return
	}

	br.Lock()
	br.parts[partNumber] = part
	br.Unlock()
}

func (s *Stream) read(id, start, end uint64) (r io.ReadCloser, err error) {
	r, w := io.Pipe()

	go func() {
		for i := start; i < end; i++ {
			p := formatPath(id, i)
			_, err := s.upper.Read(p, w)
			if err != nil {
				s.errch <- err
				return
			}
		}
		err := w.Close()
		if err != nil {
			s.errch <- err
			return
		}
	}()

	return r, nil
}

func (s *Stream) complete(br *branch) {
	// Check for dirty data.
	//
	// persistedIdx < nextIdx means we still have data to write.
	if br.persistedIdx < br.nextIdx {
		start := br.persistedIdx
		end := br.nextIdx
		size := br.currentSize - br.persistedSize
		partNumber := br.nextPartNumber

		br.wg.Add(1)
		err := s.p.Submit(func() {
			defer br.wg.Done()

			s.persistViaWriteMultipart(br, start, end, size, partNumber)
		})
		if err != nil {
			s.errch <- err
			return
		}
	}

	// It's safe to complete the multipart after wait.
	br.wg.Wait()

	parts := make([]*types.Part, 0, len(br.parts))
	for i := 0; i < len(br.parts); i++ {
		parts = append(parts, br.parts[i])
	}

	err := s.under.(types.Multiparter).CompleteMultipart(br.object, parts)
	if err != nil {
		s.errch <- err
		return
	}
	return
}
