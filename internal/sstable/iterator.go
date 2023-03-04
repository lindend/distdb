package sstable

import (
	"encoding/binary"
	"io"
)

type SSTableIterator struct {
	tbl             *SSTable
	key             []byte
	kind            uint64
	value           []byte
	nextIndexOffset int64
}

func (s SSTableIterator) Next() (*SSTableIterator, error) {
	if s.nextIndexOffset == int64(s.tbl.index.Len()) {
		return nil, io.EOF
	}

	numBuf := make([]byte, 8)

	_, err := s.tbl.index.ReadAt(numBuf, s.nextIndexOffset)
	if err != nil {
		return nil, err
	}
	kind := binary.BigEndian.Uint64(numBuf)

	_, err = s.tbl.index.ReadAt(numBuf, s.nextIndexOffset+8)
	if err != nil {
		return nil, err
	}
	keyLen := int64(binary.BigEndian.Uint64(numBuf))

	keyBuffer := make([]byte, keyLen)
	_, err = s.tbl.index.ReadAt(keyBuffer, s.nextIndexOffset+8+8)
	if err != nil {
		return nil, err
	}

	_, err = s.tbl.index.ReadAt(numBuf, s.nextIndexOffset+8+8+keyLen)
	if err != nil {
		return nil, err
	}
	dataOffset := int64(binary.BigEndian.Uint64(numBuf))

	dataBuffer, err := s.tbl.getDataEntry(dataOffset)

	if err != nil {
		return nil, err
	}

	return &SSTableIterator{
		tbl:             s.tbl,
		kind:            kind,
		key:             keyBuffer,
		value:           dataBuffer,
		nextIndexOffset: s.nextIndexOffset + 8 + keyLen + 8,
	}, nil
}

func (s SSTableIterator) Value() (uint64, string, []byte) {
	return s.kind, string(s.key), s.value
}
