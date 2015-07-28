package skiplist

import (
	"bytes"
	"fmt"
)

type nilItem struct {
	cmp int
}

func (i *nilItem) Compare(itm Item) int {
	return i.cmp
}

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

type intKeyItem int

func (itm *intKeyItem) String() string {
	return fmt.Sprint(*itm)
}

func (itm *intKeyItem) Compare(other Item) int {
	var otherItem *intKeyItem
	var ok bool

	if other == nil {
		return 1
	}

	if otherItem, ok = other.(*intKeyItem); !ok {
		return 1
	}

	return int(*itm) - int(*otherItem)
}
