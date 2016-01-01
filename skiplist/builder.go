package skiplist

import "sync/atomic"
import "math/rand"
import "unsafe"

type NodeCallback func(*Node)

type Segment struct {
	builder *Builder
	tail    []*Node
	head    []*Node
	rand    *rand.Rand
	callb   NodeCallback
	count   uint64
}

func (s *Segment) SetNodeCallback(fn NodeCallback) {
	s.callb = fn
}

func (s *Segment) Add(itm unsafe.Pointer) {
	itemLevel := s.builder.store.NewLevel(s.rand.Float32)
	x := newNode(itm, itemLevel)
	atomic.AddInt64(&s.builder.store.stats.levelNodesCount[itemLevel], 1)
	atomic.AddInt64(&s.builder.store.usedBytes, int64(s.builder.store.Size(x)))

	for l := 0; l <= itemLevel; l++ {
		if s.tail[l] != nil {
			s.tail[l].setNext(l, x, false)
		} else {
			s.head[l] = x
		}
		s.tail[l] = x
	}

	if s.callb != nil {
		s.callb(x)
	}
}

// Concurrent bottom-up skiplist builder
type Builder struct {
	store *Skiplist
}

func (b *Builder) SetItemSizeFunc(fn ItemSizeFn) {
	b.store.itemSize = fn
}

func (b *Builder) NewSegment() *Segment {
	return &Segment{tail: make([]*Node, MaxLevel+1),
		head: make([]*Node, MaxLevel+1), builder: b,
		rand: rand.New(rand.NewSource(int64(rand.Int()))),
	}
}

func (b *Builder) Assemble(segments ...*Segment) *Skiplist {
	tail := make([]*Node, MaxLevel+1)
	head := make([]*Node, MaxLevel+1)

	for _, seg := range segments {
		for l := 0; l <= MaxLevel; l++ {
			if tail[l] != nil && seg.head[l] != nil {
				tail[l].setNext(l, seg.head[l], false)
			} else if head[l] == nil && seg.head[l] != nil {
				head[l] = seg.head[l]
			}

			if seg.tail[l] != nil {
				tail[l] = seg.tail[l]
			}
		}
	}

	for l := 0; l <= MaxLevel; l++ {
		if head[l] != nil {
			b.store.head.setNext(l, head[l], false)
		}
		if tail[l] != nil {
			tail[l].setNext(l, b.store.tail, false)
		}
	}

	return b.store

}

func NewBuilder() *Builder {
	return &Builder{store: New()}
}
