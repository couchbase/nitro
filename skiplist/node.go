// +build !amd64

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
	"sync/atomic"
	"unsafe"
)

//
// The default skiplist implementation :
// a) should support 64-bit platforms including aarch64 which has alignment restrictions
// (on arm64, 64-bit words accessed atomically should have 64-bit address alignment
// otherwise will result in alignment fault.)
// b) and can work with both golang memory (garbage collection safe) as well as user
// managed memory (e.g. jemalloc)
//
// Node layout:
// Nitro's two-phase deletion approach requires us to atomically update both the
// next pointer as well as the state of the current node.
//
// For user-managed memory, this can be addressed by using tagged pointers. However
// this relies on the fact that the platform's virtual addresses do not consume the
// entire 64 bits. To our knowledge, amd64 uses 48-bits VA addresses and for ARMV8.3
// supporting large VA addressing mode (64K page size), it can go upto 52-bits. This
// has the advantage of using word aligned operations which is a requirement for
// certain platforms.
//
// Below is the node layout we use for user managed memory
//
//  <NodeMM struct>
// +------------+-----------+-----------+-------------+
// | level - 8b | itm - 8b  |  Link - 8b|  Cache - 8b |<NodeRefMM>
// +------------+-----------+-----------+-------------+-------------+--------------+
//                                                    | tag ptr- 8b| tag ptr - 8b|
//                                                    +-------------+--------------+
//
// For golang memory, the same can be addressed by using an indirection pointer. A
// NodeRef pointer is stored in skiplist levels which point to an object which contains
// both the state & the next pointer. This is the existing implementation.
//
// Below is the node layout we use for golang memory
//
//  <Node struct>
// +------------+------------+-----------+-------------+-------------+
// | level - 8b | next - 8b  | itm - 8b  |  Link - 8b  |  Cache - 8b |
// +------------+------------+-----------+-------------+-------------+
//              | ----- |------------------+----------------+
//			| NodeRef ptr - 8b | NodeRefptr - 8b|
//                      |------------------+----------------+
//
// Note: Although golang indirection approach can work with user managed memory,
// but it comes with an overhead of constant memory allocation/deallocation in
// case of conflicts and also SMR will not be straightforward. Also reclaim in SMR
// becomes easy if we allocate node memory as a single blob (NodeMM).
//
// Based on memory config used for skiplist, we cache the type information in the
// MSB of level field to save extra bytes. Currently MaxLevel is 32. But it can go
// up to 2^63 -1
//

// 52-bit Large VA address capability is supported from ARMv8.2 onwards (64KB page size)
const deletedFlag = uint64(1) << 52
const deletedFlagMask = ^deletedFlag

// memory management type, bit set for user managed memory
const mmFlag = int(1) << 62
const mmFlagMask = (^mmFlag)

var nodeHdrSizeMM = unsafe.Sizeof(NodeMM{})
var nodeRefSizeMM = unsafe.Sizeof(NodeRefMM{})

// Node represents skiplist entry
// This should reside in a single cache line (L1 cache 64bytes)
type Node struct {
	level int // we use the 2nd highest bit to store memory type
	itm   unsafe.Pointer
	Link  unsafe.Pointer
	Cache int64          // needed by plasma
	next  unsafe.Pointer // Points to [level+1]unsafe.Pointer
}

// NodeRef is a wrapper for node pointer
type NodeRef struct {
	deleted bool
	ptr     *Node
}

// NodeMM represents skiplist entry from user managed memory.
// We skips the next pointer in Node struct to save bytes
type NodeMM struct {
	level int // // we use the 63rd bit to store node type
	itm   unsafe.Pointer
	Link  unsafe.Pointer
	Cache int64 // needed by plasma
}

// NodeRefMM is a wrapper for Node(MM) pointer tagged with deletedFlag
type NodeRefMM struct {
	tagptr uint64
}

// for user managed memory
func (n *Node) setMM() {
	n.level |= mmFlag
}

// this is inlined by go as seen from profile
func (n *Node) usesMM() bool {
	return (n.level & mmFlag) != 0
}

// get a slice of NodeRef's containing golang pointers
func (n *Node) nextArray() (s []unsafe.Pointer) {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	hdr.Data = uintptr(n.next)
	hdr.Len = n.Level() + 1
	hdr.Cap = hdr.Len
	return
}

// Level returns the level of a node in the skiplist
func (n Node) Level() int {
	return n.level & mmFlagMask
}

// Size returns memory used by the node
func (n Node) Size() int {
	if n.usesMM() {
		return int(nodeHdrSizeMM + uintptr(n.Level()+1)*nodeRefSizeMM)
	} else {
		return int(unsafe.Sizeof(n) +
			uintptr(n.Level()+1)*(unsafe.Sizeof(unsafe.Pointer(nil))+
				unsafe.Sizeof(NodeRef{})))
	}
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

func allocNode(itm unsafe.Pointer, level int, fn MallocFn) *Node {
	var n *Node
	// we reserve level's MSB bit to cache node type
	if level < 0 || level >= mmFlag {
		return nil
	}
	if fn == nil {
		next := make([]unsafe.Pointer, level+1)
		n = &Node{
			level: level,
			next:  unsafe.Pointer(&next[0]),
		}
	} else {
		// NodeMM is casted as Node (NodeMM is not undersized)
		n = (*Node)(fn(int(nodeHdrSizeMM + uintptr(level+1)*nodeRefSizeMM)))
		if n == nil {
			return nil
		}
		n.level = level
		n.Link = nil
		n.setMM() // malloced memory
	}

	n.Cache = 0
	n.itm = itm
	return n
}

func (n *Node) setNext(level int, ptr *Node, deleted bool) {
	if n.usesMM() {
		nodeRefAddr := uintptr(unsafe.Pointer(uintptr(unsafe.Pointer(n)) +
			nodeHdrSizeMM + nodeRefSizeMM*uintptr(level)))
		wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr))
		tag := uint64(uintptr(unsafe.Pointer(ptr)))
		if deleted {
			tag |= deletedFlag
		}
		atomic.StoreUint64(wordAddr, tag)
	} else {
		next := n.nextArray()
		next[level] = unsafe.Pointer(&NodeRef{ptr: ptr, deleted: deleted})
	}
}

// GetNext returns next node in level 0
func (n *Node) GetNext() *Node {
	var next *Node
	var del bool

	for next, del = n.getNext(0); del; next, del = next.getNext(0) {
	}
	return next
}

func (n *Node) getNext(level int) (*Node, bool) {
	if n.usesMM() {
		nodeRefAddr := uintptr(unsafe.Pointer(n)) + nodeHdrSizeMM + nodeRefSizeMM*uintptr(level)
		wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr))
		v := atomic.LoadUint64(wordAddr)
		ptr := (*Node)(unsafe.Pointer(uintptr(v) & uintptr(deletedFlagMask)))
		if ptr != nil {
			return ptr, (v&deletedFlag != uint64(0))
		}
	} else {
		next := n.nextArray()
		ref := (*NodeRef)(atomic.LoadPointer(&next[level]))
		if ref != nil {
			return ref.ptr, ref.deleted
		}
	}
	return nil, false
}

func (n *Node) dcasNext(level int, prevPtr, newPtr *Node, prevIsdeleted, newIsdeleted bool) bool {
	var swapped bool
	if n.usesMM() {
		nodeRefAddr := uintptr(unsafe.Pointer(n)) + nodeHdrSizeMM + nodeRefSizeMM*uintptr(level)
		wordAddr := (*uint64)(unsafe.Pointer(nodeRefAddr))
		prevVal := uint64(uintptr(unsafe.Pointer(prevPtr)))
		newVal := uint64(uintptr(unsafe.Pointer(newPtr)))

		if prevIsdeleted {
			prevVal |= deletedFlag
		}

		if newIsdeleted {
			newVal |= deletedFlag
		}
		swapped = atomic.CompareAndSwapUint64(wordAddr, prevVal, newVal)
	} else {
		next := n.nextArray()
		addr := &next[level]
		ref := (*NodeRef)(atomic.LoadPointer(addr))
		if (ref == nil) || (ref.ptr == prevPtr && ref.deleted == prevIsdeleted) {
			swapped = atomic.CompareAndSwapPointer(addr, unsafe.Pointer(ref),
				unsafe.Pointer(&NodeRef{ptr: newPtr, deleted: newIsdeleted}))
		}
	}

	return swapped
}

// This can help debugging of memory reclaimer bugs
func debugMarkFree(n *Node) {
}
