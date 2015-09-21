package skiplist

import (
	"bytes"
	"fmt"
	"unsafe"
)

func compare(cmp CompareFn, this, that Item) int {
	if this == nil {
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

func NewByteKeyItem(k []byte) Item {
	itm := byteKeyItem(k)
	return &itm
}

func CompareBytes(this Item, that Item) int {
	thisItem := this.(*byteKeyItem)
	thatItem := that.(*byteKeyItem)
	return bytes.Compare([]byte(*thisItem), []byte(*thatItem))
}

type intKeyItem int

func (itm *intKeyItem) String() string {
	return fmt.Sprint(*itm)
}

func (itm intKeyItem) Size() int {
	return int(unsafe.Sizeof(itm))
}

func CompareInt(this Item, that Item) int {
	thisItem := this.(*intKeyItem)
	thatItem := that.(*intKeyItem)
	return int(*thisItem - *thatItem)
}
