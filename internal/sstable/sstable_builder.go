package sstable

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path"

	"github.com/bits-and-blooms/bloom/v3"
)

const bloomFalsePositiveRate = 0.01

const (
	dataEntry     byte = 0x01
	checksumEntry byte = 0x13
)

// Create a sparse index entry every x kb of the index file
const defaultSparseIndexBlockSize = 8 * 1024

// Builder to create new SSTables. Write entries to this and then call .Build() to
// create a new SSTable.
type SSTableBuilder struct {
	// Used to quickly filter queries for elements that definitely does not exist
	// in the table.
	filter *bloom.BloomFilter
	// Handle to the file where data entries are stored
	data *os.File
	// Buffered writer
	dataWriter *bufio.Writer
	// Position in the data stream where writing of next element begins. Used for
	// writing only.
	dataPosition int64
	// Handle to the file where the full index is stored. The index associates
	// keys with values in the data file.
	index *os.File
	// Buffered writer
	indexWriter *bufio.Writer
	// Position in the index stream where writing of the next element begins. Used
	// for writing only.
	indexPosition int64
	// An in-memory sparsely populated version of the on disk index. Offset here points to
	// the next entry in the index file.
	sparseIndex sparseIndex
	// How large blocks in the index file to tolerate before generating a new entry in the
	// sparse index.
	sparseIndexBlockSize int64
	// Flag indicating that a sparse index and bloom filter has been set up. Set to true
	// on Build and when a SSTable is loaded from disk. With the flag true the SSTable
	// can not accept new writes.
	built bool
	// The last key that was added to the SSTable, used to detect out-of-order writes.
	previousKey string
	// Root directory of SSTables
	root string
	// Name of this SSTable
	name string
	// Metadata
	meta SSTableMetaData
}

// Creates a new SSTableBuilder. numElements is the approximate number of elements that will be stored,
// it's used to set up the bloom filter.
func NewSSTable(numElements uint, root string, name string) (*SSTableBuilder, error) {
	data, err := os.Create(path.Join(root, name+dataFileExtension))
	if err != nil {
		return nil, err
	}

	index, err := os.Create(path.Join(root, name+indexFileExtension))
	if err != nil {
		return nil, err
	}

	return &SSTableBuilder{
		filter:               bloom.NewWithEstimates(numElements, bloomFalsePositiveRate),
		data:                 data,
		dataWriter:           bufio.NewWriter(data),
		dataPosition:         0,
		index:                index,
		indexWriter:          bufio.NewWriter(index),
		indexPosition:        0,
		sparseIndex:          sparseIndex{},
		sparseIndexBlockSize: defaultSparseIndexBlockSize,
		previousKey:          "",
		root:                 root,
		name:                 name,
		built:                false,
		meta:                 SSTableMetaData{NumEntries: 0},
	}, nil
}

func (s *SSTableBuilder) saveBloomFilter() error {
	file, err := os.Create(path.Join(s.root, s.name+bloomFilterFileExtension))
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = s.filter.WriteTo(file)
	return err
}

func (s *SSTableBuilder) saveSparseIndex() error {
	file, err := os.Create(path.Join(s.root, s.name+sparseIndexFileExtension))
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	err = encoder.Encode(s.sparseIndex)
	if err != nil {
		return err
	}

	return nil
}

func (s *SSTableBuilder) saveMetadata() error {
	file, err := os.Create(path.Join(s.root, s.name+metadataFileExtension))
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	err = encoder.Encode(s.meta)
	if err != nil {
		return err
	}

	return nil
}

func (s *SSTableBuilder) writeIndexEntry(key []byte, kind uint64) (int64, error) {
	bytesWritten := int64(0)
	numBuf := make([]byte, 8)

	// Write kind of entry
	binary.BigEndian.PutUint64(numBuf, kind)
	n, err := s.indexWriter.Write(numBuf)
	if err != nil {
		return 0, err
	}
	bytesWritten += int64(n)

	// Write size of key
	keyLen := len(key)
	binary.BigEndian.PutUint64(numBuf, uint64(keyLen))
	n, err = s.indexWriter.Write(numBuf)
	if err != nil {
		return 0, err
	}
	bytesWritten += int64(n)

	// Write key
	n, err = s.indexWriter.Write(key)
	if err != nil {
		return 0, err
	}
	bytesWritten += int64(n)

	// Write offset in data file where data will be written
	binary.BigEndian.PutUint64(numBuf, uint64(s.dataPosition))
	n, err = s.indexWriter.Write(numBuf)
	if err != nil {
		return 0, err
	}
	bytesWritten += int64(n)

	return bytesWritten, nil
}

func (s *SSTableBuilder) writeDataEntry(data []byte) (int64, error) {
	bytesWritten := int64(0)
	numBuf := make([]byte, 8)

	n, err := s.dataWriter.Write([]byte{dataEntry})
	if err != nil {
		return 0, err
	}
	bytesWritten += int64(n)

	// Write size of data
	dataLen := len(data)
	binary.BigEndian.PutUint64(numBuf, uint64(dataLen))
	n, err = s.dataWriter.Write(numBuf)
	if err != nil {
		return 0, err
	}
	bytesWritten += int64(n)

	// Write the data
	n, err = s.dataWriter.Write(data)
	if err != nil {
		return 0, err
	}
	bytesWritten += int64(n)

	return bytesWritten, nil
}

func (s *SSTableBuilder) currentSparseIndexBlockSize() int64 {
	if len(s.sparseIndex) == 0 {
		// Force a sparse index at position 0
		return s.sparseIndexBlockSize
	} else {
		lastSparseIndex := s.sparseIndex[len(s.sparseIndex)-1]
		return s.indexPosition - lastSparseIndex.Offset
	}
}

// Writes a new entry to the SSTable. Entries must be added in ascending order. The table
// cannot have been built. A table loaded from disk cannot have additional entries added.
func (s *SSTableBuilder) Write(key string, kind uint64, data []byte) error {
	if s.built {
		return errors.New("cannot write to a built SSTable, data structure is immutable")
	}

	if s.previousKey > key {
		return errors.New("must add keys in ascending order to SSTable")
	}
	s.previousKey = key

	if s.sparseIndexBlockSize <= s.currentSparseIndexBlockSize() {
		s.sparseIndex = append(s.sparseIndex, indexEntry{
			Key:    key,
			Offset: s.indexPosition,
		})
	}

	keyBytes := []byte(key)
	indexBytesWritten, err := s.writeIndexEntry(keyBytes, kind)
	if err != nil {
		return err
	}
	s.indexPosition += indexBytesWritten

	dataBytesWritten, err := s.writeDataEntry(data)
	if err != nil {
		return err
	}
	s.dataPosition += int64(dataBytesWritten)

	s.filter.Add(keyBytes)
	s.meta.NumEntries += 1

	return nil
}

// Saves everything to disk and returns a new SSTable
// ready for reading.
func (s *SSTableBuilder) Build() (*SSTable, error) {
	if s.built {
		return nil, errors.New("sstable already built")
	}

	if err := s.saveBloomFilter(); err != nil {
		return nil, err
	}

	if err := s.saveSparseIndex(); err != nil {
		return nil, err
	}

	if err := s.saveMetadata(); err != nil {
		return nil, err
	}

	if err := s.dataWriter.Flush(); err != nil {
		return nil, err
	}

	if err := s.indexWriter.Flush(); err != nil {
		return nil, err
	}

	if err := s.data.Sync(); err != nil {
		return nil, err
	}
	if err := s.index.Sync(); err != nil {
		return nil, err
	}

	s.built = true

	s.data.Close()
	s.index.Close()

	return LoadSSTable(s.root, s.name)
}
