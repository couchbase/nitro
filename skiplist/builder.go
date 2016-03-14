package skiplist

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

	sts Stats
}

func (s *Segment) SetNodeCallback(fn NodeCallback) {
	s.callb = fn
}

func (s *Segment) Add(itm unsafe.Pointer) {
	itemLevel := s.builder.store.NewLevel(s.rand.Float32)
	x := s.builder.store.newNode(itm, itemLevel)
	s.sts.AddInt64(&s.sts.nodeAllocs, 1)
	s.sts.AddInt64(&s.sts.levelNodesCount[itemLevel], 1)
	s.sts.AddInt64(&s.sts.usedBytes, int64(s.builder.store.Size(x)))

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
	b.store.ItemSize = fn
}

func (b *Builder) NewSegment() *Segment {
	seg := &Segment{tail: make([]*Node, MaxLevel+1),
		head: make([]*Node, MaxLevel+1), builder: b,
		rand: rand.New(rand.NewSource(int64(rand.Int()))),
	}

	seg.sts.IsLocal(true)
	return seg
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

	for _, seg := range segments {
		b.store.Stats.Merge(&seg.sts)
	}

	return b.store

}

func NewBuilder() *Builder {
	return NewBuilderWithConfig(DefaultConfig())
}

func NewBuilderWithConfig(cfg Config) *Builder {
	return &Builder{store: NewWithConfig(cfg)}
}
