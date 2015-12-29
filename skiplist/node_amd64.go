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
	itm    unsafe.Pointer
	GClink *Node
}{})

var nodeRefSize = unsafe.Sizeof(NodeRef{})

var nodeRefFlagSize = unsafe.Sizeof(NodeRef{}.flag)

const deletedFlag = 0xff

type Node struct {
	itm    unsafe.Pointer
	GClink *Node
	level  uint16
}

func (n Node) Level() int {
	return int(n.level)
}

func (n Node) Size() int {
	return int(nodeHdrSize + uintptr(n.level+1)*nodeRefSize)
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
