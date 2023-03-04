package sstable

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path"

	"github.com/bits-and-blooms/bloom/v3"
	"golang.org/x/exp/mmap"
)

const dataFileExtension = ".data"
const indexFileExtension = ".index"
const metadataFileExtension = ".meta"
const bloomFilterFileExtension = ".bloom"
const sparseIndexFileExtension = ".spindex"

type indexEntry struct {
	Key    string `json:"k"`
	Offset int64  `json:"o"`
}

type SSTableMetaData struct {
	NumEntries int64
}

type sparseIndex []indexEntry

// Immutable data structure used for quick key-value lookups from disk.
type SSTable struct {
	// Used to quickly filter queries for elements that definitely does not exist
	// in the table.
	filter *bloom.BloomFilter
	// Handle to the file where data entries are stored
	data *mmap.ReaderAt
	// Handle to the file where the full index is stored. The index associates
	// keys with values in the data file.
	index *mmap.ReaderAt
	// An in-memory sparsely populated version of the on disk index. Offset here points to
	// the next entry in the index file.
	sparseIndex sparseIndex
	// Root directory of SSTables
	root string
	// Name of this SSTable
	name string
	// Metadata
	meta SSTableMetaData
}

func loadBloomFilter(root string, name string) (*bloom.BloomFilter, error) {
	file, err := os.Open(path.Join(root, name+bloomFilterFileExtension))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	bloomFilter := bloom.BloomFilter{}
	bloomFilter.ReadFrom(file)

	return &bloomFilter, nil
}

func loadSparseIndex(root string, name string) (sparseIndex, error) {
	file, err := os.Open(path.Join(root, name+sparseIndexFileExtension))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	idx := sparseIndex{}
	err = decoder.Decode(&idx)
	if err != nil {
		return nil, err
	}

	return idx, nil
}

func loadMetadata(root string, name string) (*SSTableMetaData, error) {
	file, err := os.Open(path.Join(root, name+metadataFileExtension))
	if err != nil {
		return nil, err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	m := SSTableMetaData{}
	err = decoder.Decode(&m)
	if err != nil {
		return nil, err
	}

	return &m, nil
}

// Loads an SSTable from disk. An SSTable is stored in a few different files:
// .index - all the keys, with offsets pointing to the .data file
// .data - data of the SSTable
// .bloom - a bloom filter used to quickly reject items not in this SSTable
// .spindex - the sparse index, which is loaded into memory. Used to
//
//	look up actual index locations in the index file.
func LoadSSTable(root string, name string) (*SSTable, error) {
	bloomFilter, err := loadBloomFilter(root, name)
	if err != nil {
		return nil, err
	}

	sparseIndex, err := loadSparseIndex(root, name)
	if err != nil {
		return nil, err
	}

	metadata, err := loadMetadata(root, name)
	if err != nil {
		return nil, err
	}

	data, err := mmap.Open(path.Join(root, name+dataFileExtension))
	if err != nil {
		return nil, err
	}

	index, err := mmap.Open(path.Join(root, name+indexFileExtension))
	if err != nil {
		return nil, err
	}

	sstable := &SSTable{
		filter:      bloomFilter,
		data:        data,
		index:       index,
		sparseIndex: sparseIndex,
		meta:        *metadata,
		root:        root,
		name:        name,
	}

	return sstable, nil
}

// Performs a lookup in the sparse index to determine range of index offsets where
// the key can be present.
func (s *SSTable) getIndexRange(key string) (start int64, end int64) {
	min := 0
	max := len(s.sparseIndex) - 1

	// Last segment is a special case
	if s.sparseIndex[max].Key <= key {
		return s.sparseIndex[max].Offset, int64(s.index.Len())
	}

	// Perform a binary search over the index blocks
	for max != min {
		// Divide by 2 rounded up
		idx := (min+max)/2 + (min+max)%2
		v := s.sparseIndex[idx]
		if v.Key > key {
			max = idx - 1
		} else if v.Key < key {
			min = idx
		} else {
			return s.sparseIndex[idx].Offset, s.sparseIndex[idx+1].Offset
		}
	}
	return s.sparseIndex[min].Offset, s.sparseIndex[min+1].Offset
}

// Scans the on disk index from byte offsets start to end looking for the specified key.
// Returns the offset in the data file where result can be found
func (s *SSTable) scanIndex(key []byte, start int64, end int64) (uint64, int64, bool, error) {
	// TODO: pool the buffer
	buffer := make([]byte, end-start)

	// Load the whole range we are interested of into a buffer
	_, err := s.index.ReadAt(buffer, start)
	if err != nil {
		return 0, 0, false, err
	}

	// Start looking for the key
	for i := int64(0); i < end-start; {
		kind := binary.BigEndian.Uint64(buffer[i : i+8])
		i += 8
		keyLen := int64(binary.BigEndian.Uint64(buffer[i : i+8]))
		i += 8
		bufferKey := buffer[i : i+keyLen]
		// Check if it's the correct key
		if bytes.Equal(bufferKey, key) {
			offset := binary.BigEndian.Uint64(buffer[i+keyLen : i+keyLen+8])
			return kind, int64(offset), true, nil
		}
		i += keyLen + 8
	}
	return 0, 0, false, nil
}

func (s *SSTable) getDataEntry(offset int64) ([]byte, error) {
	kindBuf := make([]byte, 1)
	_, err := s.data.ReadAt(kindBuf, offset)
	if err != nil {
		return nil, err
	}
	// TODO: act on kind of entry (checksum, etc)
	// kind := kindBuf[0]

	numBuf := make([]byte, 8)
	_, err = s.data.ReadAt(numBuf, offset+1)
	if err != nil {
		return nil, err
	}
	dataLen := binary.BigEndian.Uint64(numBuf)
	data := make([]byte, dataLen)
	_, err = s.data.ReadAt(data, offset+1+8)
	return data, err
}

func (s *SSTable) Read(key string) (uint64, []byte, bool, error) {
	if !s.filter.Test([]byte(key)) {
		return 0, nil, false, nil
	}

	keyBytes := []byte(key)

	indexStart, indexEnd := s.getIndexRange(key)

	kind, dataOffset, exists, err := s.scanIndex(keyBytes, indexStart, indexEnd)

	if err != nil {
		return 0, nil, false, err
	}

	if !exists {
		return 0, nil, false, nil
	}

	data, err := s.getDataEntry(dataOffset)

	if err != nil {
		return 0, nil, false, err
	}

	return kind, data, true, nil
}

func (s *SSTable) Size() (int64, error) {
	return int64(s.data.Len()), nil
}

func (s *SSTable) Close() error {
	err := s.data.Close()
	err2 := s.index.Close()
	return errors.Join(err, err2)
}

func (s *SSTable) NumEntries() int64 {
	return s.meta.NumEntries
}

func (s *SSTable) Iterator() (*SSTableIterator, error) {
	it := SSTableIterator{
		tbl:             s,
		key:             nil,
		value:           nil,
		nextIndexOffset: 0,
	}
	return it.Next()
}

func (s *SSTable) Delete() error {
	s.data.Close()
	s.index.Close()
	os.Remove(path.Join(s.root, s.name+bloomFilterFileExtension))
	os.Remove(path.Join(s.root, s.name+dataFileExtension))
	os.Remove(path.Join(s.root, s.name+indexFileExtension))
	os.Remove(path.Join(s.root, s.name+sparseIndexFileExtension))
	os.Remove(path.Join(s.root, s.name+metadataFileExtension))
	return nil
}
