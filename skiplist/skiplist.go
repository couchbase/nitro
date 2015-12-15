package skiplist

import (
	"math/rand"
	"sync/atomic"
	"unsafe"
)

const MaxLevel = 32
const p = 0.25

type Item interface {
	Size() int
}

type CompareFn func(Item, Item) int

type Skiplist struct {
	head      *Node
	tail      *Node
	level     int32
	stats     stats
	usedBytes int64

	// Do not use item size for usedBytes computation
	ignoreItemSize bool
}

func New() *Skiplist {
	head := newNode(nil, MaxLevel)
	tail := newNode(nil, MaxLevel)

	for i := 0; i <= MaxLevel; i++ {
		head.setNext(i, tail, false)
	}

	s := &Skiplist{
		head: head,
		tail: tail,
	}

	return s
}

func (s *Skiplist) IgnoreItemSize() {
	s.ignoreItemSize = true
}

type ActionBuffer struct {
	preds []*Node
	succs []*Node
}

func (s *Skiplist) MakeBuf() *ActionBuffer {
	return &ActionBuffer{
		preds: make([]*Node, MaxLevel+1),
		succs: make([]*Node, MaxLevel+1),
	}
}

// TODO: Use sync pool
func (s *Skiplist) FreeBuf(b *ActionBuffer) {
}

type Node struct {
	next   []unsafe.Pointer
	itm    atomic.Value
	GClink *Node
}

func (n Node) Level() int {
	return int(len(n.next) - 1)
}

func (s *Skiplist) ResetItem(n *Node, itm Item) (deltaSz int) {
	if !s.ignoreItemSize {
		deltaSz = itm.Size() - n.Item().Size()
	}
	n.itm.Store(itm)

	return
}

func (n *Node) Item() Item {
	itm := n.itm.Load()
	if itm != nil {
		return itm.(Item)
	}
	return nil
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

func newNode(itm Item, level int) *Node {
	n := &Node{
		next: make([]unsafe.Pointer, level+1),
	}

	if itm != nil {
		n.itm.Store(itm)
	}

	return n
}

func (n *Node) setNext(level int, ptr *Node, deleted bool) {
	n.next[level] = unsafe.Pointer(&NodeRef{ptr: ptr, deleted: deleted})
}

func (n *Node) getNext(level int) (*Node, bool) {
	ref := (*NodeRef)(atomic.LoadPointer(&n.next[level]))
	if ref != nil {
		return ref.ptr, ref.deleted
	}

	return nil, false
}

func (n *Node) dcasNext(level int, prevPtr, newPtr *Node, prevIsdeleted, newIsdeleted bool) bool {
	var swapped bool
	addr := &n.next[level]
	ref := (*NodeRef)(atomic.LoadPointer(addr))
	if ref != nil {
		if ref.ptr == prevPtr && ref.deleted == prevIsdeleted {
			swapped = atomic.CompareAndSwapPointer(addr, unsafe.Pointer(ref),
				unsafe.Pointer(&NodeRef{ptr: newPtr, deleted: newIsdeleted}))
		}
	}

	return swapped
}

func (s *Skiplist) Size(n *Node) int {
	var ref NodeRef
	var next *Node

	itmSz := uintptr(0)

	if !s.ignoreItemSize {
		itmSz = uintptr(n.Item().Size())
	}

	return int(
		unsafe.Sizeof(n.next) +
			unsafe.Sizeof(n.itm) +
			itmSz +
			unsafe.Sizeof(n.GClink) +
			(unsafe.Sizeof(next)+unsafe.Sizeof(ref))*uintptr(n.Level()+1))
}

func (s *Skiplist) AdjustUsedBytes(adj int) {
	atomic.AddInt64(&s.usedBytes, -int64(adj))
}

func (s *Skiplist) NewLevel(randFn func() float32) int {
	var nextLevel int

	for ; randFn() < p; nextLevel++ {
	}

	if nextLevel > MaxLevel {
		nextLevel = MaxLevel
	}

	level := int(atomic.LoadInt32(&s.level))
	if nextLevel > level {
		if atomic.CompareAndSwapInt32(&s.level, int32(level), int32(level+1)) {
			nextLevel = level + 1
		} else {
			nextLevel = level
		}
	}

	return nextLevel
}

func (s *Skiplist) helpDelete(level int, prev, curr, next *Node) bool {
	success := prev.dcasNext(level, curr, next, false, false)
	if success && level == curr.Level() {
		atomic.AddInt64(&s.stats.softDeletes, -1)
		atomic.AddInt64(&s.stats.levelNodesCount[level], -1)
		atomic.AddInt64(&s.usedBytes, -int64(s.Size(curr)))
	}
	return success
}

func (s *Skiplist) FindPath(itm Item, cmp CompareFn,
	buf *ActionBuffer) (foundNode *Node) {
	var cmpVal int = 1

retry:
	prev := s.head
	level := int(atomic.LoadInt32(&s.level))
	for i := level; i >= 0; i-- {
		curr, _ := prev.getNext(i)
	levelSearch:
		for {
			next, deleted := curr.getNext(i)
			for deleted {
				if !s.helpDelete(i, prev, curr, next) {
					atomic.AddUint64(&s.stats.readConflicts, 1)
					goto retry
				}

				curr, _ = prev.getNext(i)
				next, deleted = curr.getNext(i)
			}

			cmpVal = compare(cmp, curr.Item(), itm)
			if cmpVal < 0 {
				prev = curr
				curr, _ = prev.getNext(i)
			} else {
				break levelSearch
			}
		}

		buf.preds[i] = prev
		buf.succs[i] = curr
	}

	if cmpVal == 0 {
		foundNode = buf.succs[0]
	}
	return
}

func (s *Skiplist) Insert(itm Item, cmp CompareFn, buf *ActionBuffer) (success bool) {
	_, success = s.Insert2(itm, cmp, buf, rand.Float32)
	return
}

func (s *Skiplist) Insert2(itm Item, cmp CompareFn,
	buf *ActionBuffer, randFn func() float32) (*Node, bool) {
	itemLevel := s.NewLevel(randFn)
	return s.Insert3(itm, cmp, buf, itemLevel, false)
}

func (s *Skiplist) Insert3(itm Item, cmp CompareFn,
	buf *ActionBuffer, itemLevel int, skipFindPath bool) (*Node, bool) {

	x := newNode(itm, itemLevel)
	atomic.AddInt64(&s.stats.levelNodesCount[itemLevel], 1)
	atomic.AddInt64(&s.usedBytes, int64(s.Size(x)))

retry:
	if skipFindPath {
		skipFindPath = false
	} else {
		if s.FindPath(itm, cmp, buf) != nil {
			return nil, false
		}
	}

	x.setNext(0, buf.succs[0], false)
	if !buf.preds[0].dcasNext(0, buf.succs[0], x, false, false) {
		atomic.AddUint64(&s.stats.insertConflicts, 1)
		goto retry
	}

	for i := 1; i <= int(itemLevel); i++ {
	fixThisLevel:
		for {
			x.setNext(i, buf.succs[i], false)
			if buf.preds[i].dcasNext(i, buf.succs[i], x, false, false) {
				break fixThisLevel
			}
			s.FindPath(itm, cmp, buf)
		}
	}

	return x, true
}

func (s *Skiplist) softDelete(delNode *Node) bool {
	var deleteMarked bool

	targetLevel := delNode.Level()
	for i := targetLevel; i >= 0; i-- {
		next, deleted := delNode.getNext(i)
		for !deleted {
			deleteMarked = delNode.dcasNext(i, next, next, false, true)
			next, deleted = delNode.getNext(i)
		}
	}

	if deleteMarked {
		atomic.AddInt64(&s.stats.softDeletes, 1)
	}

	return deleteMarked
}

func (s *Skiplist) Delete(itm Item, cmp CompareFn, buf *ActionBuffer) bool {
	found := s.FindPath(itm, cmp, buf) != nil
	if !found {
		return false
	}

	delNode := buf.succs[0]
	return s.DeleteNode(delNode, cmp, buf)
}

func (s *Skiplist) DeleteNode(n *Node, cmp CompareFn, buf *ActionBuffer) bool {
	itm := n.Item()
	if s.softDelete(n) {
		s.FindPath(itm, cmp, buf)
		return true
	}

	return false
}

func (s *Skiplist) GetRangeSplitItems(nways int) []Item {
	var deleted bool
repeat:
	var itms []Item
	var finished bool

	l := int(atomic.LoadInt32(&s.level))
	for ; l >= 0; l-- {
		c := int(atomic.LoadInt64(&s.stats.levelNodesCount[l]) + 1)
		if c >= nways {
			perSplit := c / nways
			node := s.head
			for j := 0; node != s.tail && !finished; j++ {
				if j == perSplit {
					j = -1
					itms = append(itms, node.Item())
					finished = len(itms) == nways-1
				}

				node, deleted = node.getNext(l)
				if deleted {
					goto repeat
				}
			}

			break
		}
	}

	return itms
}
