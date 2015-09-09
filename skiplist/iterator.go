package skiplist

type Iterator struct {
	cmp        CompareFn
	s          *Skiplist
	prev, curr *Node
	valid      bool
	buf        *ActionBuffer
	deleted    bool
}

func (s *Skiplist) NewIterator(cmp CompareFn,
	buf *ActionBuffer) *Iterator {

	return &Iterator{
		cmp: cmp,
		s:   s,
		buf: buf,
	}
}

func (it *Iterator) SeekFirst() {
	it.prev = it.s.head
	it.curr, _ = it.s.head.getNext(0)
	it.valid = true
}

func (it *Iterator) Seek(itm Item) bool {
	it.valid = true
	found := it.s.findPath(itm, it.cmp, it.buf)
	it.prev = it.buf.preds[0]
	it.curr = it.buf.succs[0]
	return found
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

func (it *Iterator) GetNode() *Node {
	return it.curr
}

func (it *Iterator) Delete() {
	it.s.softDelete(it.curr)
	// It will observe that current item is deleted
	// Run delete helper and move to the next possible item
	it.Next()
	it.deleted = true
}

func (it *Iterator) Next() {
	if it.deleted {
		it.deleted = false
		return
	}

retry:
	it.valid = true
	next, deleted := it.curr.getNext(0)
	for deleted {
		if !it.s.helpDelete(0, it.prev, it.curr, next) {
			found := it.s.findPath(it.curr.itm, it.cmp, it.buf)
			last := it.curr
			it.prev = it.buf.preds[0]
			it.curr = it.buf.succs[0]
			if found && last == it.curr {
				goto retry
			} else {
				return
			}
		}
		it.curr, _ = it.prev.getNext(0)
		next, deleted = it.curr.getNext(0)
	}

	it.prev = it.curr
	it.curr = next
}
