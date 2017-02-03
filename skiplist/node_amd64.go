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
	"sync/atomic"
	"unsafe"
)

// Node structure overlaps with an array of NodeRef struct
//
//  <Node struct>
// +--------------+-----------------+----------------+
// | itm - 8bytes | GClink - 8bytes | level = 2 bytes|  <[]NodeRef struct>
// +--------------+-----------------+----------------+-----+--------------+--------------+--------------+
//                                  | flag - 8bytes        | ptr - 8 bytes| flag - 8bytes| ptr - 8 bytes|
//                                  +----------------------+--------------+--------------+--------------+

var nodeHdrSize = unsafe.Sizeof(struct {
	itm     unsafe.Pointer
	GClink  *Node
	DataPtr unsafe.Pointer
}{})

var nodeRefSize = unsafe.Sizeof(NodeRef{})

var nodeRefFlagSize = unsafe.Sizeof(NodeRef{}.flag)

const deletedFlag = 0xff

// Node represents skiplist node header
type Node struct {
	itm   unsafe.Pointer
	Link  unsafe.Pointer
	Cache int64
	level uint16
}

// Level returns the level of a node in the skiplist
func (n Node) Level() int {
	return int(n.level)
}

// Size returns memory used by the node
func (n Node) Size() int {
	return int(nodeHdrSize + uintptr(n.level+1)*nodeRefSize)
}

// Item returns item held by the node
func (n *Node) Item() unsafe.Pointer {
	return n.itm
}

// SetItem sets itm ptr
func (n *Node) SetItem(itm unsafe.Pointer) {
	n.itm = itm
}

// SetLink can be used to set link pointer for the node
func (n *Node) SetLink(l *Node) {
	n.Link = unsafe.Pointer(l)
}

// GetLink returns link pointer from the node
func (n *Node) GetLink() *Node {
	return (*Node)(n.Link)
}

// GetNext returns next node in level 0
func (n *Node) GetNext() *Node {
	var next *Node
	var del bool

	for next, del = n.getNext(0); del; next, del = next.getNext(0) {
	}

	return next
}

// NodeRef is a wrapper for node pointer
type NodeRef struct {
	flag uint64
	ptr  *Node
}

func (n *Node) setNext(level int, ptr *Node, deleted bool) {
	nlevel := n.level
	ref := (*NodeRef)(unsafe.Pointer(uintptr(unsafe.Pointer(n)) + nodeHdrSize + nodeRefSize*uintptr(level)))
	ref.ptr = ptr
	ref.flag = 0
	// Setting flag for level 0 will require reseting of level
	if level == 0 {
		n.level = nlevel
	}
}

func (n *Node) getNext(level int) (*Node, bool) {
	nodeRefAddr := uintptr(unsafe.Pointer(n)) + nodeHdrSize + nodeRefSize*uintptr(level)
	wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr + uintptr(7)))

	v := atomic.LoadUint64(wordAddr)
	deleted := v&deletedFlag == deletedFlag
	ptr := (*Node)(unsafe.Pointer(uintptr(v >> 8)))
	return ptr, deleted
}

// The node struct holds a slice of NodeRef. We assume that the
// most-significant-byte of the golang pointer is always unused. In NodeRef
// struct, deleted flag and *Node are packed one after the other.
// If we shift the node address 1 byte to the left. The shifted 8 byte word will have
// a byte from the deleted flag and 7 bytes from the address (8th byte of the address
// is always 0x00). CAS operation can be performed at this location to set
// least-significant to 0xff (denotes deleted). Same applies for loading delete
// flag and the address atomically.
func (n *Node) dcasNext(level int, prevPtr, newPtr *Node, prevIsdeleted, newIsdeleted bool) bool {
	nodeRefAddr := uintptr(unsafe.Pointer(n)) + nodeHdrSize + nodeRefSize*uintptr(level)
	wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr + uintptr(7)))
	prevVal := uint64(uintptr(unsafe.Pointer(prevPtr)) << 8)
	newVal := uint64(uintptr(unsafe.Pointer(newPtr)) << 8)

	if newIsdeleted {
		newVal |= deletedFlag
	}

	swapped := atomic.CompareAndSwapUint64(wordAddr, prevVal, newVal)

	// This is required to make go1.5+ concurrent garbage collector happy
	// It makes writebarrier to mark newPtr as reachable
	if swapped {
		atomic.CompareAndSwapPointer((*unsafe.Pointer)(unsafe.Pointer(nodeRefAddr+nodeRefFlagSize)),
			unsafe.Pointer(newPtr), unsafe.Pointer(newPtr))
	}

	return swapped
}
