package skiplist

import (
	"bytes"
	"math/rand"
)

const MaxLevel = 32
const p = 0.25

type Item interface {
	Compare(Item) int
}

type byteKeyItem struct {
	key []byte
}

func (itm *byteKeyItem) String() string {
	return string(itm.key)
}

func NewByteKeyItem(k []byte) Item {
	return &byteKeyItem{
		key: k,
	}
}

func (itm *byteKeyItem) Compare(other Item) int {
	var otherItem *byteKeyItem
	var ok bool

	if other == nil {
		return 1
	}

	if otherItem, ok = other.(*byteKeyItem); !ok {
		return 1
	}

	return bytes.Compare(itm.key, otherItem.key)
}

type nilItem struct {
	cmp int
}

func (i *nilItem) Compare(itm Item) int {
	return i.cmp
}

type Skiplist struct {
	head  *Node
	tail  *Node
	level uint16
}

func New() *Skiplist {
	minItem := &nilItem{
		cmp: -1,
	}

	maxItem := &nilItem{
		cmp: 1,
	}

	head := newNode(minItem, MaxLevel)
	tail := newNode(maxItem, MaxLevel)

	for i := 0; i <= MaxLevel; i++ {
		head.next[i] = tail
	}

	s := &Skiplist{
		head: head,
		tail: tail,
	}

	return s
}

type Node struct {
	next  []*Node
	itm   Item
	level uint16
}

func newNode(itm Item, level uint16) *Node {
	return &Node{
		next:  make([]*Node, level+1),
		itm:   itm,
		level: level,
	}
}

func (s *Skiplist) randomLevel() (uint16, bool) {
	var nextLevel uint16 = 0
	var created bool

	for ; rand.Float32() < p; nextLevel++ {
	}

	if nextLevel > MaxLevel {
		nextLevel = MaxLevel
	}

	if nextLevel > s.level {
		s.level += 1
		nextLevel = s.level
		created = true
	}

	return nextLevel, created
}

func (s Skiplist) findPath(itm Item) (preds, succs []*Node, found bool) {
	var cmpVal int = 1

	preds = make([]*Node, MaxLevel+1)
	succs = make([]*Node, MaxLevel+1)
	prev := s.head
loop1:
	for i := int(s.level); i >= 0; i-- {
		curr := prev.next[i]
		if curr == nil {
			break loop1
		}
	loop2:
		for {
			cmpVal = curr.itm.Compare(itm)
			if cmpVal < 0 {
				prev = curr
				curr = prev.next[i]
			} else {
				break loop2
			}
		}

		if cmpVal == 0 {
			found = true
		}

		preds[i] = prev
		succs[i] = curr
	}

	return
}

func (s *Skiplist) Insert(itm Item) {
	preds, succs, _ := s.findPath(itm)
	itemLevel, created := s.randomLevel()
	x := newNode(itm, itemLevel)

	if created {
		preds[itemLevel] = s.head
		succs[itemLevel] = s.tail
	}

	for i := 0; i <= int(itemLevel); i++ {
		x.next[i] = succs[i]
		preds[i].next[i] = x
	}
}

func (s *Skiplist) Delete(itm Item) {
	preds, succs, found := s.findPath(itm)
	if !found {
		return
	}

	for i := 0; i <= int(s.level); i++ {

		if itm.Compare(preds[i].next[i].itm) != 0 {
			break
		}

		preds[i].next[i] = succs[i].next[i]
	}

	if s.head.next[s.level] == s.tail {
		s.level--
	}
}

type Iterator struct {
	s     *Skiplist
	curr  *Node
	valid bool
}

func (s *Skiplist) NewIterator() *Iterator {
	return &Iterator{
		s: s,
	}
}

func (it *Iterator) SeekFirst() {
	it.curr = it.s.head.next[0]
	it.valid = true
}

func (it *Iterator) Seek(itm Item) {
	it.valid = true
	_, succs, found := it.s.findPath(itm)
	it.curr = succs[0]
	if !found {
		it.valid = false
	}
}

func (it *Iterator) Valid() bool {
	if it.valid && it.curr == it.s.tail {
		it.valid = false
	}

	return it.valid
}

func (it *Iterator) Get() Item {
	return it.curr.itm
}

func (it *Iterator) Next() {
	it.valid = true
	it.curr = it.curr.next[0]
}
