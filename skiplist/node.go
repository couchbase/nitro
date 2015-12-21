// +build !amd64

package skiplist

import (
	"reflect"
)

type Node struct {
	level  int
	next   unsafe.Pointer // Points to [level+1]unsafe.Pointer
	itm    unsafe.Pointer
	GClink *Node
}

func (n *Node) nextArray() (s []unsafe.Pointer) {
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&s))
	hdr.Data = uintptr(n.next)
	hdr.Len = n.Level() + 1
	hdr.Cap = hdr.Len
	return
}

func (n Node) Level() int {
	return n.level
}

func (n Node) Size() int {
	return int(unsafe.Sizeof(n) +
		uintptr(n.level+1)*(unsafe.Sizeof(unsafe.Pointer(0))+
			unsafe.Sizeof(NodeRef{})))
}

func (n *Node) Item() unsafe.Pointer {
	return n.itm
}

func (n *Node) SetLink(l *Node) {
	n.GClink = l
}

func (n *Node) GetLink() *Node {
	return n.GClink
}

type NodeRef struct {
	deleted bool
	ptr     *Node
}

func newNode(itm unsafe.Pointer, level int) *Node {
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
