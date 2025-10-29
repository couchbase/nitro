// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package skiplist

import (
	"sync/atomic"
	"unsafe"
)

// Memory Layout of <Node struct>:
//
// Without padding:
// Node structure overlaps with an array of NodeRef struct
// +--------------+---------------+----------------+----------------+
// | itm = 8bytes | Link = 8bytes | Cache = 8bytes | level = 2bytes |
// +--------------+---------------+----------------+----------------+--+---------------+ ... +---------------+---------------+
// <Node struct>                                   |   flag = 8bytes   | ptr = 8 bytes | ... | flag = 8bytes | ptr = 8 bytes |
//                                                 +-------------------+---------------+ ... +---------------+---------------+
//                                                 <[]NodeRef struct>
//
// With 8byte padding:
// +--------------+---------------+----------------+----------------+------------------+
// | itm = 8bytes | Link = 8bytes | Cache = 8bytes | level = 2bytes | PADDING = 6bytes |
// +--------------+---------------+----------------+----------------+------------------+---------------+---------------+ ... +---------------+---------------+
// <Node struct>                                                                       | flag = 8bytes | ptr = 8 bytes | ... | flag = 8bytes | ptr = 8 bytes |
//                                                                                     +---------------+---------------+ ... +---------------+---------------+
//                                                                                     <[]NodeRef struct>

// If a NodeRef straddles a cache line, then the CAS performed in dcasNext
// will incur a split lock. To avoid the split locking penalties, pad the
// node structs so that NodeRef is aligned on 16 bytes. Note that jemalloc
// gives 16-byte aligned pointers.
const PADDING = 8

var nodeHdrSizeNotPadded = unsafe.Sizeof(struct {
	itm     unsafe.Pointer
	GClink  *Node
	DataPtr unsafe.Pointer
}{})

var nodeHdrSizePadded = nodeHdrSizeNotPadded + PADDING

var nodeRefSize = unsafe.Sizeof(NodeRef{})

var nodeRefFlagSize = unsafe.Sizeof(NodeRef{}.flag)

const deletedFlag = 0xff
const usingPaddingFlag = uint64(0x1 << 47)

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

// NodeRef's flag is 8 bytes, but only last byte is used.
// For the level 0, the first two bytes overlap with level if no padding used
// If padding is used, then there is no overlap.
// Use 1 bit right after level in this unused span to indicate whether padding is used or not.
func (n *Node) setUsePadding(usePadding bool) {
	levelPtr := (*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(n)) + nodeHdrSizeNotPadded))

	if usePadding {
		*levelPtr |= usingPaddingFlag
	} else {
		*levelPtr &= (^usingPaddingFlag)
	}
}

func (n *Node) IsUsingPadding() bool {
	levelPtr := (*uint64)(unsafe.Pointer(uintptr(unsafe.Pointer(n)) + nodeHdrSizeNotPadded))
	return *levelPtr&usingPaddingFlag != 0
}

func (n *Node) getHdrSize() uintptr {
	nhs := nodeHdrSizeNotPadded
	if n.IsUsingPadding() {
		nhs = nodeHdrSizePadded
	}

	return nhs
}

// Size returns memory used by the node
func (n Node) Size() int {
	return int(n.getHdrSize() + uintptr(n.level+1)*nodeRefSize)
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

	usePadding := n.IsUsingPadding()
	nlevel := n.level

	ref := (*NodeRef)(unsafe.Pointer(uintptr(unsafe.Pointer(n)) + n.getHdrSize() + nodeRefSize*uintptr(level)))
	ref.ptr = ptr
	ref.flag = 0

	// Setting flag for level 0 will require reseting of level and padding bit
	if level == 0 {
		n.level = nlevel
		n.setUsePadding(usePadding)
	}
}

func (n *Node) getNext(level int) (*Node, bool) {
	nodeRefAddr := uintptr(unsafe.Pointer(n)) + n.getHdrSize() + nodeRefSize*uintptr(level)
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
	nodeRefAddr := uintptr(unsafe.Pointer(n)) + n.getHdrSize() + nodeRefSize*uintptr(level)
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
