package stream

import (
	"bytes"
	"sync"

	"github.com/beyondstorage/go-storage/v4/types"
	"github.com/panjf2000/ants/v2"
)

type Stream struct {
	upper types.Storager
	under types.Storager

	p  *ants.Pool
	ch chan op

	branches   map[uint64]*branch
	branchLock sync.Mutex
}

type op struct {
	id   uint64
	size int64
}

type branch struct {
	lock sync.Mutex
	wg   *sync.WaitGroup

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
	s.branches[id] = newBranch(id, path)
	s.branchLock.Unlock()
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

func (s *Stream) EndBranch(id uint64) (err error) {
	s.branchLock.Lock()
	br := s.branches[id]
	s.branchLock.Unlock()

	err = s.complete(br)
	if err != nil {
		return
	}

	s.branchLock.Lock()
	delete(s.branches, id)
	s.branchLock.Unlock()
	return
}

func (s *Stream) Serve() {
}

func (s *Stream) complete(br *branch) (err error) {
	return
}
