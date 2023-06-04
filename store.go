package zstore

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

const (
	// number of bytes used to store a record's length
	lenWidth = 8
)

var (
	// binary.BigEndian is the big-endian implementation of ByteOrder.
	// enc encoding of persisted record sizes and index entries
	enc = binary.BigEndian
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore returns a new store using file.
func newStore(file *os.File) (*store, error) {
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	return &store{
		File: file,
		size: uint64(info.Size()),
		buf:  bufio.NewWriter(file),
	}, nil
}

// Append appends the record to the store.
func (s *store) Append(record []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// record length
	err = binary.Write(s.buf, enc, uint64(len(record)))
	if err != nil {
		return 0, 0, err
	}

	// record
	pos = s.size
	w, err := s.buf.Write(record)
	if err != nil {
		return 0, 0, err
	}
	w += lenWidth // record length field
	s.size += uint64(w)

	return uint64(w), pos, nil
}

// Read reads the record at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	record := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(record, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return record, nil
}

// Close closes the store.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
