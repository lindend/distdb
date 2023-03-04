package lsmtree

type chunkType int

const (
	chunkTypeSkiplist chunkType = 1
	chunkTypeSSTable  chunkType = 2
)

type chunkIterator interface {
	next() chunkIterator
	value() (uint64, string, []byte)
}

type chunk struct {
	name      string
	data      chunkData
	chunkType chunkType
}

type chunkData interface {
	get(key string) (uint64, []byte, bool, error)
	set(key string, kind uint64, data []byte) error
	size() uint64
	iterator() chunkIterator
	numEntries() int64
	delete() error
}
