// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package nitro

import "os"
import "bufio"
import "errors"

var (
	// DiskBlockSize - backup file reader and writer
	DiskBlockSize     = 512 * 1024
	errNotEnoughSpace = errors.New("Not enough space in the buffer")
)

// FileType describes backup file format
type FileType int

const (
	encodeBufSize = 4
	readerBufSize = 10000
	// RawdbFile - backup file storage format
	RawdbFile FileType = iota
)

// FileWriter represents backup file writer
type FileWriter interface {
	Open(path string) error
	WriteItem(*Item) error
	Checksum() uint32
	Close() error
}

// FileReader represents backup file reader
type FileReader interface {
	Open(path string) error
	ReadItem() (*Item, error)
	Checksum() uint32
	Close() error
}

func (m *Nitro) newFileWriter(t FileType) FileWriter {
	var w FileWriter
	if t == RawdbFile {
		w = &rawFileWriter{db: m}
	}
	return w
}

func (m *Nitro) newFileReader(t FileType, ver int) FileReader {
	var r FileReader
	if t == RawdbFile {
		r = &rawFileReader{db: m, version: ver}
	}
	return r
}

type rawFileWriter struct {
	db       *Nitro
	fd       *os.File
	w        *bufio.Writer
	buf      []byte
	path     string
	checksum uint32
}

func (f *rawFileWriter) Open(path string) error {
	var err error
	f.fd, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE, 0755)
	if err == nil {
		f.buf = make([]byte, encodeBufSize)
		f.w = bufio.NewWriterSize(f.fd, DiskBlockSize)
	}
	return err
}

func (f *rawFileWriter) WriteItem(itm *Item) error {
	checksum, err := f.db.EncodeItem(itm, f.buf, f.w)
	f.checksum = f.checksum ^ checksum
	return err
}

func (f *rawFileWriter) Checksum() uint32 {
	return f.checksum
}

func (f *rawFileWriter) Close() error {
	terminator := &Item{}

	if err := f.WriteItem(terminator); err != nil {
		return err
	}

	f.w.Flush()
	return f.fd.Close()
}

type rawFileReader struct {
	version  int
	db       *Nitro
	fd       *os.File
	r        *bufio.Reader
	buf      []byte
	path     string
	checksum uint32
}

func (f *rawFileReader) Open(path string) error {
	var err error
	f.fd, err = os.Open(path)
	if err == nil {
		f.buf = make([]byte, encodeBufSize)
		f.r = bufio.NewReaderSize(f.fd, DiskBlockSize)
	}
	return err
}

func (f *rawFileReader) ReadItem() (*Item, error) {
	itm, checksum, err := f.db.DecodeItem(f.version, f.buf, f.r)
	if itm != nil { // Checksum excludes terminal nil item
		f.checksum = f.checksum ^ checksum
	}
	return itm, err
}

func (f *rawFileReader) Checksum() uint32 {
	return f.checksum
}

func (f *rawFileReader) Close() error {
	return f.fd.Close()
}
