package wal

import (
	. "testing"

	"github.com/stretchr/testify/assert"
)

const WalOperationDelete = 2
const WalOperationWrite = 6

func TestWal(t *T) {
	w, err := NewWAL("wal_test.log")

	assert.Nil(t, err, "Got error %v", err)

	w.Write(WalOperationWrite, "key1", []byte("data1"))
	w.Write(WalOperationWrite, "key2", []byte("data2"))
	w.Write(WalOperationDelete, "key3", []byte("data3"))
	w.Close()

	ws, err := LoadWAL("wal_test.log")
	assert.Nil(t, err, "Got error %v", err)

	assert.Equal(t, WALEntry{
		WalOperationWrite,
		"key1",
		[]byte("data1"),
	}, ws[0])

	assert.Equal(t, WALEntry{
		WalOperationWrite,
		"key2",
		[]byte("data2"),
	}, ws[1])

	assert.Equal(t, WALEntry{
		WalOperationDelete,
		"key3",
		[]byte("data3"),
	}, ws[2])

	assert.Nil(t, w.Delete())
}
