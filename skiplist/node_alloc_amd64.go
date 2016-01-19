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
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [1]NodeRef
}

var node1 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [2]NodeRef
}

var node2 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [3]NodeRef
}

var node3 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [4]NodeRef
}

var node4 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [5]NodeRef
}

var node5 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [6]NodeRef
}

var node6 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [7]NodeRef
}

var node7 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [8]NodeRef
}

var node8 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [9]NodeRef
}

var node9 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [10]NodeRef
}

var node10 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [11]NodeRef
}
var node11 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [12]NodeRef
}

var node12 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [13]NodeRef
}

var node13 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [14]NodeRef
}

var node14 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [15]NodeRef
}

var node15 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [16]NodeRef
}

var node16 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [17]NodeRef
}

var node17 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [18]NodeRef
}

var node18 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [19]NodeRef
}

var node19 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [20]NodeRef
}

var node20 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [21]NodeRef
}

var node21 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [22]NodeRef
}

var node22 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [23]NodeRef
}

var node23 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [24]NodeRef
}

var node24 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [25]NodeRef
}

var node25 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [26]NodeRef
}

var node26 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [27]NodeRef
}

var node27 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [28]NodeRef
}

var node28 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [29]NodeRef
}

var node29 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [30]NodeRef
}

var node30 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [31]NodeRef
}
var node31 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [32]NodeRef
}

var node32 struct {
	itm unsafe.Pointer
	gc  unsafe.Pointer
	buf [33]NodeRef
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
	n.GClink = nil
	return n
}
