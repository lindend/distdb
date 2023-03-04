package wal

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
)

type WALOperation int

const entrySeparator = '\n'

type WALEntry struct {
	Kind uint64
	Key  string
	Data []byte
}

type WAL struct {
	file     *os.File
	fileName string
}

func NewWAL(fileName string) (*WAL, error) {
	file, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return nil, err
	}
	return &WAL{
		file,
		fileName,
	}, nil
}

// Loads a WAL file, oldest entries are first in the array
func LoadWAL(fileName string) ([]WALEntry, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}

	defer file.Close()

	entries := make([]WALEntry, 0)
	r := bufio.NewReader(file)
	for {
		data, ioErr := r.ReadBytes(entrySeparator)
		if ioErr != nil && ioErr != io.EOF {
			return nil, ioErr
		}

		if len(data) > 0 {
			entry := WALEntry{}
			err = json.Unmarshal(data, &entry)
			if err != nil {
				return nil, err
			}

			entries = append(entries, entry)
		}

		if ioErr == io.EOF {
			break
		}
	}

	return entries, nil
}

func (w *WAL) Write(kind uint64, key string, data []byte) error {
	we := WALEntry{
		Kind: kind,
		Key:  key,
		Data: data,
	}
	bs, err := json.Marshal(we)
	if err != nil {
		return err
	}
	bs = append(bs, entrySeparator)

	_, err = w.file.Write(bs)
	if err != nil {
		return err
	}

	return nil
}

func (w *WAL) Close() error {
	return w.file.Close()
}

func (w *WAL) Delete() error {
	w.Close()
	return os.Remove(w.fileName)
}
