// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

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
	Close() error
}

// FileReader represents backup file reader
type FileReader interface {
	Open(path string) error
	ReadItem() (*Item, error)
	Close() error
}

func (m *Nitro) newFileWriter(t FileType) FileWriter {
	var w FileWriter
	if t == RawdbFile {
		w = &rawFileWriter{db: m}
	}
	return w
}

func (m *Nitro) newFileReader(t FileType) FileReader {
	var r FileReader
	if t == RawdbFile {
		r = &rawFileReader{db: m}
	}
	return r
}

type rawFileWriter struct {
	db   *Nitro
	fd   *os.File
	w    *bufio.Writer
	buf  []byte
	path string
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
	return f.db.EncodeItem(itm, f.buf, f.w)
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
	db   *Nitro
	fd   *os.File
	r    *bufio.Reader
	buf  []byte
	path string
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
	return f.db.DecodeItem(f.buf, f.r)
}

func (f *rawFileReader) Close() error {
	return f.fd.Close()
}
