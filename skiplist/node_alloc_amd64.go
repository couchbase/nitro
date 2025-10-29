//
// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package skiplist

import (
	"reflect"
	"unsafe"
)

// SKIPLIST NODE STRUCTS - NO PADDING

var nodeTypesNotPadded = [33]reflect.Type{
	reflect.TypeOf(node0),
	reflect.TypeOf(node1),
	reflect.TypeOf(node2),
	reflect.TypeOf(node3),
	reflect.TypeOf(node4),
	reflect.TypeOf(node5),
	reflect.TypeOf(node6),
	reflect.TypeOf(node7),
	reflect.TypeOf(node8),
	reflect.TypeOf(node9),
	reflect.TypeOf(node10),
	reflect.TypeOf(node11),
	reflect.TypeOf(node12),
	reflect.TypeOf(node13),
	reflect.TypeOf(node14),
	reflect.TypeOf(node15),
	reflect.TypeOf(node16),
	reflect.TypeOf(node17),
	reflect.TypeOf(node18),
	reflect.TypeOf(node19),
	reflect.TypeOf(node20),
	reflect.TypeOf(node21),
	reflect.TypeOf(node22),
	reflect.TypeOf(node23),
	reflect.TypeOf(node24),
	reflect.TypeOf(node25),
	reflect.TypeOf(node26),
	reflect.TypeOf(node27),
	reflect.TypeOf(node28),
	reflect.TypeOf(node29),
	reflect.TypeOf(node30),
	reflect.TypeOf(node31),
	reflect.TypeOf(node32),
}

var node0 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [1]NodeRef
}

var node1 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [2]NodeRef
}

var node2 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [3]NodeRef
}

var node3 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [4]NodeRef
}

var node4 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [5]NodeRef
}

var node5 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [6]NodeRef
}

var node6 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [7]NodeRef
}

var node7 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [8]NodeRef
}

var node8 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [9]NodeRef
}

var node9 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [10]NodeRef
}

var node10 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [11]NodeRef
}
var node11 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [12]NodeRef
}

var node12 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [13]NodeRef
}

var node13 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [14]NodeRef
}

var node14 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [15]NodeRef
}

var node15 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [16]NodeRef
}

var node16 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [17]NodeRef
}

var node17 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [18]NodeRef
}

var node18 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [19]NodeRef
}

var node19 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [20]NodeRef
}

var node20 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [21]NodeRef
}

var node21 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [22]NodeRef
}

var node22 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [23]NodeRef
}

var node23 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [24]NodeRef
}

var node24 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [25]NodeRef
}

var node25 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [26]NodeRef
}

var node26 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [27]NodeRef
}

var node27 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [28]NodeRef
}

var node28 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [29]NodeRef
}

var node29 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [30]NodeRef
}

var node30 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [31]NodeRef
}
var node31 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [32]NodeRef
}

var node32 struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	buf   [33]NodeRef
}

// SKIPLIST NODE STRUCTS - WITH PADDING

var nodeTypesPadded = [33]reflect.Type{
	reflect.TypeOf(node0p),
	reflect.TypeOf(node1p),
	reflect.TypeOf(node2p),
	reflect.TypeOf(node3p),
	reflect.TypeOf(node4p),
	reflect.TypeOf(node5p),
	reflect.TypeOf(node6p),
	reflect.TypeOf(node7p),
	reflect.TypeOf(node8p),
	reflect.TypeOf(node9p),
	reflect.TypeOf(node10p),
	reflect.TypeOf(node11p),
	reflect.TypeOf(node12p),
	reflect.TypeOf(node13p),
	reflect.TypeOf(node14p),
	reflect.TypeOf(node15p),
	reflect.TypeOf(node16p),
	reflect.TypeOf(node17p),
	reflect.TypeOf(node18p),
	reflect.TypeOf(node19p),
	reflect.TypeOf(node20p),
	reflect.TypeOf(node21p),
	reflect.TypeOf(node22p),
	reflect.TypeOf(node23p),
	reflect.TypeOf(node24p),
	reflect.TypeOf(node25p),
	reflect.TypeOf(node26p),
	reflect.TypeOf(node27p),
	reflect.TypeOf(node28p),
	reflect.TypeOf(node29p),
	reflect.TypeOf(node30p),
	reflect.TypeOf(node31p),
	reflect.TypeOf(node32p),
}

var node0p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [1]NodeRef
}

var node1p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [2]NodeRef
}

var node2p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [3]NodeRef
}

var node3p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [4]NodeRef
}

var node4p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [5]NodeRef
}

var node5p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [6]NodeRef
}

var node6p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [7]NodeRef
}

var node7p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [8]NodeRef
}

var node8p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [9]NodeRef
}

var node9p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [10]NodeRef
}

var node10p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [11]NodeRef
}
var node11p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [12]NodeRef
}

var node12p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [13]NodeRef
}

var node13p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [14]NodeRef
}

var node14p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [15]NodeRef
}

var node15p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [16]NodeRef
}

var node16p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [17]NodeRef
}

var node17p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [18]NodeRef
}

var node18p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [19]NodeRef
}

var node19p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [20]NodeRef
}

var node20p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [21]NodeRef
}

var node21p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [22]NodeRef
}

var node22p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [23]NodeRef
}

var node23p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [24]NodeRef
}

var node24p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [25]NodeRef
}

var node25p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [26]NodeRef
}

var node26p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [27]NodeRef
}

var node27p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [28]NodeRef
}

var node28p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [29]NodeRef
}

var node29p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [30]NodeRef
}

var node30p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [31]NodeRef
}
var node31p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [32]NodeRef
}

var node32p struct {
	itm   unsafe.Pointer
	gc    unsafe.Pointer
	cache int64
	_     [PADDING]byte
	buf   [33]NodeRef
}

func allocNode(itm unsafe.Pointer, level int, malloc MallocFn, usePadding bool) *Node {

	nt := nodeTypesNotPadded
	if usePadding {
		nt = nodeTypesPadded
	}

	var block unsafe.Pointer
	if malloc == nil {
		block = unsafe.Pointer(reflect.New(nt[level]).Pointer())
	} else {
		block = malloc(int(nt[level].Size()))
	}

	n := (*Node)(block)
	n.level = uint16(level)
	n.itm = itm
	n.Link = nil
	n.Cache = 0

	n.setUsePadding(usePadding)

	return n
}

var freeBlockContent []byte

func init() {
	l := int(nodeTypesPadded[32].Size())
	freeBlockContent = make([]byte, l)
	for i := 0; i < l; i++ {
		freeBlockContent[i] = 0xdd
	}
}

// Fill free blocks with a const
// This can help debugging of memory reclaimer bugs
func debugMarkFree(n *Node) {

	nt := nodeTypesNotPadded
	if n.IsUsingPadding() {
		nt = nodeTypesPadded
	}

	var block []byte
	l := int(nt[n.level].Size())
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&block))
	sh.Data = uintptr(unsafe.Pointer(n))
	sh.Len = l
	sh.Cap = l

	copy(block, freeBlockContent)
}
