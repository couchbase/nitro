package memdb

import (
	"encoding/binary"
	"io"
	"reflect"
	"unsafe"
)

type Item struct {
	bornSn  uint32
	deadSn  uint32
	dataLen uint32
}

func NewItem(data []byte) *Item {
	itm := newItem(len(data))
	copy(itm.Bytes(), data)
	return itm
}

func newItem(l int) *Item {
	blockSize := round(unsafe.Sizeof(Item{})+uintptr(l), unsafe.Sizeof(uintptr(0)))
	block := make([]byte, blockSize)
	itm := (*Item)(unsafe.Pointer(&block[0]))
	itm.dataLen = uint32(l)
	return itm
}

func (itm *Item) Encode(buf []byte, w io.Writer) error {
	l := 2
	if len(buf) < l {
		return ErrNotEnoughSpace
	}

	binary.BigEndian.PutUint16(buf[0:2], uint16(itm.dataLen))
	if _, err := w.Write(buf[0:2]); err != nil {
		return err
	}
	if _, err := w.Write(itm.Bytes()); err != nil {
		return err
	}

	return nil
}

func (x *Item) Decode(buf []byte, r io.Reader) (*Item, error) {
	if _, err := io.ReadFull(r, buf[0:2]); err != nil {
		return nil, err
	}

	l := binary.BigEndian.Uint16(buf[0:2])
	if l > 0 {
		itm := newItem(int(l))
		data := itm.Bytes()
		_, err := io.ReadFull(r, data)
		return itm, err
	}

	return x, nil
}

func (itm *Item) Bytes() (bs []byte) {
	l := itm.dataLen
	dataOffset := uintptr(unsafe.Pointer(itm)) + unsafe.Sizeof(Item{})

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	hdr.Data = dataOffset
	hdr.Len = int(l)
	hdr.Cap = hdr.Len
	return
}

func ItemSize(p unsafe.Pointer) int {
	itm := (*Item)(p)
	return int(round(unsafe.Sizeof(*itm)+uintptr(itm.dataLen), unsafe.Sizeof(uintptr(0))))
}

func round(size, tomul uintptr) uintptr {
	return (size + (tomul - 1)) & ^(tomul - 1)
}
