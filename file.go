package memdb

import "os"
import "bufio"
import "errors"
import "github.com/couchbase/goforestdb"
import "bytes"

const DiskBlockSize = 512 * 1024

var (
	ErrNotEnoughSpace = errors.New("Not enough space in the buffer")
	forestdbConfig    *forestdb.Config
)

func init() {
	forestdbConfig = forestdb.DefaultConfig()
	forestdbConfig.SetSeqTreeOpt(forestdb.SEQTREE_NOT_USE)
	forestdbConfig.SetBufferCacheSize(1024 * 1024)

}

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
	var w FileWriter
	if t == RawdbFile {
		w = &rawFileWriter{}
	} else if t == ForestdbFile {
		w = &forestdbFileWriter{}
	}
	return w
}

func newFileReader(t FileType) FileReader {
	var r FileReader
	if t == RawdbFile {
		r = &rawFileReader{}
	} else if t == ForestdbFile {
		r = &forestdbFileReader{}
	}
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

type forestdbFileWriter struct {
	file  *forestdb.File
	store *forestdb.KVStore
	buf   []byte
	wbuf  bytes.Buffer
}

func (f *forestdbFileWriter) Open(path string) error {
	var err error
	f.file, err = forestdb.Open(path, forestdbConfig)
	if err == nil {
		f.buf = make([]byte, encodeBufSize)
		f.store, err = f.file.OpenKVStoreDefault(nil)
	}

	return err
}

func (f *forestdbFileWriter) WriteItem(itm *Item) error {
	f.wbuf.Reset()
	err := itm.Encode(f.buf, &f.wbuf)
	if err == nil {
		err = f.store.SetKV(f.wbuf.Bytes(), nil)
	}

	return err
}

func (f *forestdbFileWriter) Close() error {
	err := f.file.Commit(forestdb.COMMIT_NORMAL)
	if err == nil {
		err = f.store.Close()
		if err == nil {
			err = f.file.Close()
		}
	}

	return err
}

type forestdbFileReader struct {
	file  *forestdb.File
	store *forestdb.KVStore
	iter  *forestdb.Iterator
	buf   []byte
}

func (f *forestdbFileReader) Open(path string) error {
	var err error

	f.file, err = forestdb.Open(path, forestdbConfig)
	if err == nil {
		f.buf = make([]byte, encodeBufSize)
		f.store, err = f.file.OpenKVStoreDefault(nil)
		if err == nil {
			f.iter, err = f.store.IteratorInit(nil, nil, forestdb.ITR_NONE)
		}
	}

	return err
}

func (f *forestdbFileReader) ReadItem() (*Item, error) {
	itm := &Item{}
	doc, err := f.iter.Get()
	if err == forestdb.RESULT_ITERATOR_FAIL {
		return nil, nil
	}

	f.iter.Next()
	if err == nil {
		rbuf := bytes.NewBuffer(doc.Key())
		err = itm.Decode(f.buf, rbuf)
	}

	return itm, err
}

func (f *forestdbFileReader) Close() error {
	f.iter.Close()
	f.store.Close()
	return f.file.Close()
}
