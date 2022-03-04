// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package skiplist

import (
	"bytes"
	"fmt"
	"unsafe"
)

var (
	MinItem unsafe.Pointer
	MaxItem = unsafe.Pointer(^uintptr(0))
)

func compare(cmp CompareFn, this, that unsafe.Pointer) int {
	if this == MinItem || that == MaxItem {
		return -1
	}

	if this == MaxItem || that == MinItem {
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

// NewByteKeyItem creates a new item from bytes
func NewByteKeyItem(k []byte) unsafe.Pointer {
	itm := byteKeyItem(k)
	return unsafe.Pointer(&itm)
}

func NewIntKeyItem(x int) unsafe.Pointer {
	p := new(int)
	*p = x
	return unsafe.Pointer(p)
}

func IntFromItem(itm unsafe.Pointer) int {
	return int(*(*IntKeyItem)(itm))
}

// CompareBytes is a byte item comparator
func CompareBytes(this, that unsafe.Pointer) int {
	thisItem := (*byteKeyItem)(this)
	thatItem := (*byteKeyItem)(that)
	return bytes.Compare([]byte(*thisItem), []byte(*thatItem))
}

type IntKeyItem int

func (itm *IntKeyItem) String() string {
	return fmt.Sprint(*itm)
}

func (itm IntKeyItem) Size() int {
	return int(unsafe.Sizeof(itm))
}

// CompareInt is a helper integer item comparator
func CompareInt(this, that unsafe.Pointer) int {
	if this == MinItem || that == MaxItem {
		return -1
	}

	if this == MaxItem || that == MinItem {
		return 1
	}

	thisItem := (*IntKeyItem)(this)
	thatItem := (*IntKeyItem)(that)
	return int(*thisItem - *thatItem)
}
