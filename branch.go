package stream

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/beyondstorage/go-storage/v4/types"
	"go.uber.org/atomic"
)

type Branch struct {
	lock sync.Mutex
	wg   *sync.WaitGroup
	s    *Stream

	id   uint64
	path string

	persistedIdx  uint64
	persistedSize int64
	nextIdx       *atomic.Uint64
	currentSize   *atomic.Int64

	// Meta of the object.
	//
	// we can check object mode to decide use CompleteMultipart or call Write.
	object *types.Object

	// Only valid if we have already called CreateMultipart.
	parts          map[int]*types.Part
	nextPartNumber int
}

func (br *Branch) Write(idx uint64, data []byte) (n int64, err error) {
	p := formatPath(br.id, idx)

	size := int64(len(data))
	n, err = br.s.upper.Write(p, bytes.NewReader(data), size)
	if err != nil {
		return
	}

	br.wg.Add(1)
	br.s.ch <- op{
		br:   br,
		size: size,
	}
	return n, nil
}

func (br *Branch) Complete() (err error) {
	br.wg.Add(1)
	br.s.ch <- op{
		br:   br,
		done: true,
	}

	// It's safe to complete the multipart after wait.
	br.wg.Wait()

	switch br.s.method {
	case PersistMethodMultipart:
		err = br.completeViaMultipart()
		if err != nil {
			return
		}
	default:
		panic(fmt.Errorf("end Branch with invalid method: %v", br.s.method))
	}

	err = br.s.delete(br.id, 0, br.persistedIdx)
	if err != nil {
		return err
	}
	return nil
}

func (br *Branch) persist(size int64, done bool) {
	br.lock.Lock()
	defer br.lock.Unlock()

	// If this op is not marked as done, we need to update index.
	if !done {
		br.nextIdx.Inc()
		br.currentSize.Add(size)
	}

	// All data has been persisted, return directly.
	if br.currentSize.Load()-br.persistedSize == 0 {
		log.Printf("skip for no data")
		br.wg.Done()
		return
	}

	// Skip write operation if we don't have enough data and current Branch is not done.
	//
	// TODO: make we can allow user to configure the buffer size?
	if br.currentSize.Load()-br.persistedSize < 5*1024*1024 && !done {
		br.wg.Done()
		return
	}

	switch br.s.method {
	case PersistMethodMultipart:
		br.serveViaMultipart()
	default:
		br.wg.Done()
		panic(fmt.Errorf("serve with invalid method: %v", br.s.method))
	}
}
