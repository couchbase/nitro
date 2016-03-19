package skiplist

import (
	"bytes"
	"fmt"
	"unsafe"
)

var (
	minItem unsafe.Pointer
	maxItem unsafe.Pointer = unsafe.Pointer(^uintptr(0))
)

func compare(cmp CompareFn, this, that unsafe.Pointer) int {
	if this == minItem || that == maxItem {
		return -1
	}

	if this == maxItem || that == minItem {
		return 1
	}

	return cmp(this, that)
}

type byteKeyItem []byte

func (itm *byteKeyItem) String() string {
	return string(*itm)
}

func (itm byteKeyItem) Size() int {
	return len(itm)
}

func NewByteKeyItem(k []byte) unsafe.Pointer {
	itm := byteKeyItem(k)
	return unsafe.Pointer(&itm)
}

func CompareBytes(this, that unsafe.Pointer) int {
	thisItem := (*byteKeyItem)(this)
	thatItem := (*byteKeyItem)(that)
	return bytes.Compare([]byte(*thisItem), []byte(*thatItem))
}

type intKeyItem int

func (itm *intKeyItem) String() string {
	return fmt.Sprint(*itm)
}

func (itm intKeyItem) Size() int {
	return int(unsafe.Sizeof(itm))
}

func CompareInt(this, that unsafe.Pointer) int {
	thisItem := (*intKeyItem)(this)
	thatItem := (*intKeyItem)(that)
	return int(*thisItem - *thatItem)
}
