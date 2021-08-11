package stream

import (
	"github.com/beyondstorage/go-storage/v4/types"
)

func (br *Branch) serveViaMultipart() {
	start := br.persistedIdx
	end := br.nextIdx.Load()
	size := br.currentSize.Load() - br.persistedSize
	partNumber := br.nextPartNumber

	br.persistedSize = br.currentSize.Load()
	br.persistedIdx = end
	br.nextPartNumber += 1

	err := br.s.p.Submit(func() {
		br.persistViaMultipart(start, end, size, partNumber)
	})
	if err != nil {
		br.s.errch <- err
		return
	}
}

func (br *Branch) persistViaMultipart(start, end uint64, size int64, partNumber int) {
	defer br.wg.Done()

	r, err := br.s.read(br.id, start, end)
	if err != nil {
		br.s.errch <- err
		return
	}
	defer func() {
		err = r.Close()
		if err != nil {
			br.s.errch <- err
			return
		}
	}()

	_, part, err := br.s.underMultipart.WriteMultipart(br.object, r, size, partNumber)
	if err != nil {
		br.s.errch <- err
		return
	}

	br.lock.Lock()
	br.parts[partNumber] = part
	br.lock.Unlock()
}

func (br *Branch) completeViaMultipart() (err error) {
	parts := make([]*types.Part, 0, len(br.parts))
	for i := 0; i < len(br.parts); i++ {
		parts = append(parts, br.parts[i])
	}

	err = br.s.underMultipart.CompleteMultipart(br.object, parts)
	if err != nil {
		return err
	}
	return
}
