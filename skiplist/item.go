package skiplist

import (
	"bytes"
)

type byteKeyItem []byte

func (itm *byteKeyItem) String() string {
	return string(*itm)
}

func NewByteKeyItem(k []byte) Item {
	itm := byteKeyItem(k)
	return &itm
}

func (itm *byteKeyItem) Compare(other Item) int {
	var otherItem *byteKeyItem
	var ok bool

	if other == nil {
		return 1
	}

	if otherItem, ok = other.(*byteKeyItem); !ok {
		return 1
	}

	return bytes.Compare([]byte(*itm), []byte(*otherItem))
}

type nilItem struct {
	cmp int
}

func (i *nilItem) Compare(itm Item) int {
	return i.cmp
}
