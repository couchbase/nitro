// +build !amd64

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
	"sync/atomic"
	"unsafe"
)

// Node represents skiplist entry
type Node struct {
	level int
	next  unsafe.Pointer // Points to [level+1]unsafe.Pointer
	itm   unsafe.Pointer
	Link  unsafe.Pointer
}

func (n *Node) nextArray() (s []unsafe.Pointer) {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	hdr.Data = uintptr(n.next)
	hdr.Len = n.Level() + 1
	hdr.Cap = hdr.Len
	return
}

// Level returns the level of a node in the skiplist
func (n Node) Level() int {
	return n.level
}

// Size returns memory used by the node
func (n Node) Size() int {
	return int(unsafe.Sizeof(n) +
		uintptr(n.level+1)*(unsafe.Sizeof(unsafe.Pointer(nil))+
			unsafe.Sizeof(NodeRef{})))
}

// Item returns item held by the node
func (n *Node) Item() unsafe.Pointer {
	return n.itm
}

// SetLink can be used to set link pointer for the node
func (n *Node) SetLink(l *Node) {
	n.Link = unsafe.Pointer(l)
}

// GetLink returns link pointer from the node
func (n *Node) GetLink() *Node {
	return (*Node)(n.Link)
}

// NodeRef is a wrapper for node pointer
type NodeRef struct {
	deleted bool
	ptr     *Node
}

func allocNode(itm unsafe.Pointer, level int, fn MallocFn) *Node {
	next := make([]unsafe.Pointer, level+1)
	n := &Node{
		level: level,
		next:  unsafe.Pointer(&next[0]),
	}

	n.itm = itm
	return n
}

func (n *Node) setNext(level int, ptr *Node, deleted bool) {
	next := n.nextArray()
	next[level] = unsafe.Pointer(&NodeRef{ptr: ptr, deleted: deleted})
}

func (n *Node) getNext(level int) (*Node, bool) {
	next := n.nextArray()
	ref := (*NodeRef)(atomic.LoadPointer(&next[level]))
	if ref != nil {
		return ref.ptr, ref.deleted
	}

	return nil, false
}

func (n *Node) dcasNext(level int, prevPtr, newPtr *Node, prevIsdeleted, newIsdeleted bool) bool {
	var swapped bool

	next := n.nextArray()
	addr := &next[level]
	ref := (*NodeRef)(atomic.LoadPointer(addr))
	if ref != nil {
		if ref.ptr == prevPtr && ref.deleted == prevIsdeleted {
			swapped = atomic.CompareAndSwapPointer(addr, unsafe.Pointer(ref),
				unsafe.Pointer(&NodeRef{ptr: newPtr, deleted: newIsdeleted}))
		}
	}

	return swapped
}
