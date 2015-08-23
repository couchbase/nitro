package memdb

import "os"
import "bufio"
import "errors"

const DiskBlockSize = 512 * 1024

var (
	ErrNotEnoughSpace = errors.New("Not enough space in the buffer")
)

type FileWriter interface {
	Open(path string) error
	WriteItem(*Item) error
	Close() error
}

type FileReader interface {
	Open(path string) error
	ReadItem() (*Item, error)
	Close() error
}

func newFileWriter(t FileType) FileWriter {
	w := &rawFileWriter{}
	return w
}

func newFileReader(t FileType) FileReader {
	r := &rawFileReader{}
	return r
}

type rawFileWriter struct {
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
	return itm.Encode(f.buf, f.w)
}

func (f *rawFileWriter) Close() error {
	terminator := &Item{
		data: []byte(nil),
	}

	if err := f.WriteItem(terminator); err != nil {
		return err
	}

	f.w.Flush()
	return f.fd.Close()
}

type rawFileReader struct {
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
	itm := &Item{}
	err := itm.Decode(f.buf, f.r)
	if len(itm.data) == 0 {
		itm = nil
	}

	return itm, err
}

func (f *rawFileReader) Close() error {
	return f.fd.Close()
}
