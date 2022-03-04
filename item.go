// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package nitro

import (
	"bytes"
	"encoding/binary"
	"hash/crc32"
	"io"
	"reflect"
	"unsafe"
)

var itemHeaderSize = unsafe.Sizeof(Item{})

// Item represents nitro item header
// The item data is followed by the header.
// Item data is a block of bytes. The user can store key and value into a
// block of bytes and provide custom key comparator.
type Item struct {
	bornSn  uint32
	deadSn  uint32
	dataLen uint32
}

func (m *Nitro) newItem(data []byte, useMM bool) (itm *Item) {
	l := len(data)
	itm = m.allocItem(l, useMM)
	copy(itm.Bytes(), data)
	return itm
}

func (m *Nitro) freeItem(itm *Item) {
	if m.useMemoryMgmt {
		m.freeFun(unsafe.Pointer(itm))
	}
}

func (m *Nitro) allocItem(l int, useMM bool) (itm *Item) {
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

// EncodeItem encodes in [4 byte len][item_bytes] format.
func (m *Nitro) EncodeItem(itm *Item, buf []byte, w io.Writer) (
	checksum uint32, err error) {
	l := 4
	if len(buf) < l {
		return checksum, errNotEnoughSpace
	}

	binary.BigEndian.PutUint32(buf[0:4], uint32(itm.dataLen))
	if _, err = w.Write(buf[0:4]); err != nil {
		return
	}
	checksum = crc32.ChecksumIEEE(buf[0:4])
	itmBytes := itm.Bytes()
	if _, err = w.Write(itmBytes); err != nil {
		return
	}
	checksum = checksum ^ crc32.ChecksumIEEE(itmBytes)

	return
}

// DecodeItem decodes encoded item
// v0: [2 byte len][item_bytes] format.
// v1: [4 byte len][item_bytes] format.
func (m *Nitro) DecodeItem(ver int, buf []byte, r io.Reader) (*Item, uint32, error) {
	var l int
	var checksum uint32

	if ver == 0 {
		if _, err := io.ReadFull(r, buf[0:2]); err != nil {
			return nil, checksum, err
		}
		l = int(binary.BigEndian.Uint16(buf[0:2]))
		checksum = crc32.ChecksumIEEE(buf[0:2])
	} else {
		if _, err := io.ReadFull(r, buf[0:4]); err != nil {
			return nil, checksum, err
		}
		l = int(binary.BigEndian.Uint32(buf[0:4]))
		checksum = crc32.ChecksumIEEE(buf[0:4])
	}

	if l > 0 {
		itm := m.allocItem(l, m.useMemoryMgmt)
		data := itm.Bytes()
		_, err := io.ReadFull(r, data)
		if err == nil {
			checksum = checksum ^ crc32.ChecksumIEEE(data)
		}
		return itm, checksum, err
	}

	return nil, checksum, nil
}

// Bytes return item data bytes
func (itm *Item) Bytes() (bs []byte) {
	l := itm.dataLen
	dataOffset := uintptr(unsafe.Pointer(itm)) + itemHeaderSize

	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	hdr.Data = dataOffset
	hdr.Len = int(l)
	hdr.Cap = hdr.Len
	return
}

// ItemSize returns total bytes consumed by item representation
func ItemSize(p unsafe.Pointer) int {
	itm := (*Item)(p)
	return int(itemHeaderSize + uintptr(itm.dataLen))
}

// KVToBytes encodes key-value pair to item bytes which can be passed
// to the Put() and Delete() methods.
func KVToBytes(k, v []byte) []byte {
	klen := len(k)
	buf := make([]byte, 2, len(k)+len(v)+2)
	binary.LittleEndian.PutUint16(buf[0:2], uint16(klen))
	buf = append(buf, k...)
	buf = append(buf, v...)

	return buf
}

// KVFromBytes extracts key-value pair from item bytes returned by iterator
func KVFromBytes(bs []byte) (k, v []byte) {
	klen := int(binary.LittleEndian.Uint16(bs[0:2]))
	return bs[2 : 2+klen], bs[2+klen:]
}

// CompareKV is a comparator for KV item
func CompareKV(a []byte, b []byte) int {
	la := int(binary.LittleEndian.Uint16(a[0:2]))
	lb := int(binary.LittleEndian.Uint16(b[0:2]))

	return bytes.Compare(a[2:2+la], b[2:2+lb])
}
