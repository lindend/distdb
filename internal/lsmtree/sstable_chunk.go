package lsmtree

import (
	"errors"

	"github.com/lindend/distdb/internal/sstable"
)

type sstableChunk struct {
	tbl *sstable.SSTable
}

type sstableChunkIterator struct {
	it *sstable.SSTableIterator
}

func (s *sstableChunk) get(key string) (uint64, []byte, bool, error) {
	return s.tbl.Read(key)
}

func (s *sstableChunk) set(key string, kind uint64, data []byte) error {
	return errors.New("write not supported for SSTable chunk")
}

func (s *sstableChunk) size() uint64 {
	size, _ := s.tbl.Size()
	return uint64(size)
}

func (s *sstableChunk) iterator() chunkIterator {
	it, _ := s.tbl.Iterator()
	return &sstableChunkIterator{
		it: it,
	}
}

func (s *sstableChunk) numEntries() int64 {
	return s.tbl.NumEntries()
}

func (s *sstableChunk) delete() error {
	return s.tbl.Delete()
}

func (s *sstableChunkIterator) value() (uint64, string, []byte) {
	return s.it.Value()
}

func (s *sstableChunkIterator) next() chunkIterator {
	it, _ := s.it.Next()

	if it == nil {
		return nil
	}

	return &sstableChunkIterator{
		it: it,
	}
}
