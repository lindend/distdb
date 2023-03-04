package lsmtree

import (
	"github.com/lindend/distdb/internal/collections"
	"github.com/lindend/distdb/internal/wal"
)

type skiplistEntry struct {
	kind uint64
	data []byte
}

type skiplistChunkIterator struct {
	element *collections.SkiplistElement[string, skiplistEntry]
}

func (i skiplistChunkIterator) next() chunkIterator {
	next := i.element.Next()
	if next == nil {
		return nil
	}
	return skiplistChunkIterator{
		element: next,
	}
}

func (i skiplistChunkIterator) value() (kind uint64, key string, data []byte) {
	k, d := i.element.Value()
	return d.kind, *k, d.data
}

type skiplistChunk struct {
	list     collections.SkipList[string, skiplistEntry]
	wal      *wal.WAL
	dataSize uint64
}

func newSkipListChunk(fileName string) (*skiplistChunk, error) {
	// In case of shut-down or crash where skiplist has not been
	// merged to an SSTable on disk, load WAL
	walEntries, err := wal.LoadWAL(fileName)

	wal, err := wal.NewWAL(fileName)
	if err != nil {
		return nil, err
	}

	sl := &skiplistChunk{
		list:     collections.NewSkipList[string, skiplistEntry](16),
		wal:      wal,
		dataSize: 0,
	}

	// Populate existing WAL entries into skiplist
	for _, e := range walEntries {
		sl.list.Insert(e.Key, skiplistEntry{
			kind: e.Kind,
			data: e.Data,
		})
	}

	return sl, nil
}

func (l skiplistChunk) get(key string) (kind uint64, data []byte, exists bool, err error) {
	v, exists := l.list.Get(key)
	if !exists || v == nil {
		return 0, nil, false, nil
	}
	return v.kind, v.data, true, nil
}

func (l *skiplistChunk) set(key string, kind uint64, data []byte) error {
	err := l.wal.Write(kind, key, data)
	if err != nil {
		return err
	}

	oldValue := l.list.Insert(key, skiplistEntry{kind, data})
	if oldValue != nil {
		l.dataSize += uint64(len(data) - len(oldValue.data))
	} else {
		l.dataSize += uint64(len(data))
	}
	return nil
}

func (l skiplistChunk) size() uint64 {
	return l.dataSize
}

func (l skiplistChunk) iterator() chunkIterator {
	iterator := l.list.Iterate()
	if iterator == nil {
		return nil
	}

	return skiplistChunkIterator{
		element: iterator,
	}
}

func (l skiplistChunk) numEntries() int64 {
	return int64(l.list.Len())
}

func (l skiplistChunk) delete() error {
	return l.wal.Delete()
}
