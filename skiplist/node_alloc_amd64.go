// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package skiplist

import (
	"reflect"
	"unsafe"
)

var nodeTypes = [33]reflect.Type{
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

func allocNode(itm unsafe.Pointer, level int, malloc MallocFn) *Node {
	var block unsafe.Pointer
	if malloc == nil {
		block = unsafe.Pointer(reflect.New(nodeTypes[level]).Pointer())
	} else {
		block = malloc(int(nodeTypes[level].Size()))
	}

	n := (*Node)(block)
	n.level = uint16(level)
	n.itm = itm
	n.Link = nil
	n.Cache = 0
	return n
}

var freeBlockContent []byte

func init() {
	l := int(nodeTypes[32].Size())
	freeBlockContent = make([]byte, l)
	for i := 0; i < l; i++ {
		freeBlockContent[i] = 0xdd
	}
}

// Fill free blocks with a const
// This can help debugging of memory reclaimer bugs
func debugMarkFree(n *Node) {
	var block []byte
	l := int(nodeTypes[n.level].Size())
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&block))
	sh.Data = uintptr(unsafe.Pointer(n))
	sh.Len = l
	sh.Cap = l

	copy(block, freeBlockContent)
}
