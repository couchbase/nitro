package memdb

import (
	"encoding/binary"
	"io"
	"reflect"
	"unsafe"
)

var itemHeaderSize = unsafe.Sizeof(Item{})

type Item struct {
	bornSn  uint32
	deadSn  uint32
	dataLen uint32
}

func (m *MemDB) newItem(data []byte, useMM bool) (itm *Item) {
	l := len(data)
	itm = m.allocItem(l, useMM)
	copy(itm.Bytes(), data)
	return itm
}

func (m *MemDB) freeItem(itm *Item) {
	if m.useMemoryMgmt {
		m.freeFun(unsafe.Pointer(itm))
	}
}

func (m *MemDB) allocItem(l int, useMM bool) (itm *Item) {
	blockSize := itemHeaderSize + uintptr(l)
	if useMM {
		itm = (*Item)(m.mallocFun(int(blockSize)))
		itm.deadSn = 0
		itm.bornSn = 0
	} else {
		block := make([]byte, blockSize)
		itm = (*Item)(unsafe.Pointer(&block[0]))
	}

	itm.dataLen = uint32(l)
	return
}

func (m *MemDB) EncodeItem(itm *Item, buf []byte, w io.Writer) error {
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

func (m *MemDB) DecodeItem(buf []byte, r io.Reader) (*Item, error) {
	if _, err := io.ReadFull(r, buf[0:2]); err != nil {
		return nil, err
	}

	l := binary.BigEndian.Uint16(buf[0:2])
	if l > 0 {
		itm := m.allocItem(int(l), m.useMemoryMgmt)
		data := itm.Bytes()
		_, err := io.ReadFull(r, data)
		return itm, err
	}

	return nil, nil
}

func (itm *Item) Bytes() (bs []byte) {
	l := itm.dataLen
	dataOffset := uintptr(unsafe.Pointer(itm)) + itemHeaderSize

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	hdr.Data = dataOffset
	hdr.Len = int(l)
	hdr.Cap = hdr.Len
	return
}

func ItemSize(p unsafe.Pointer) int {
	itm := (*Item)(p)
	return int(itemHeaderSize + uintptr(itm.dataLen))
}
