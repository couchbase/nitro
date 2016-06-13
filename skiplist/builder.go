// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package skiplist

import "math/rand"
import "unsafe"

// NodeCallback is used by segment builder
type NodeCallback func(*Node)

// Segment is a skiplist segment
type Segment struct {
	builder *Builder
	tail    []*Node
	head    []*Node
	rand    *rand.Rand
	callb   NodeCallback
	count   uint64

	sts Stats
}

// SetNodeCallback sets callback for segment builder
func (s *Segment) SetNodeCallback(fn NodeCallback) {
	s.callb = fn
}

// Add an item into skiplist segment
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

// Builder performs concurrent bottom-up skiplist build
type Builder struct {
	store *Skiplist
}

// SetItemSizeFunc configures items size function
func (b *Builder) SetItemSizeFunc(fn ItemSizeFn) {
	b.store.ItemSize = fn
}

// NewSegment creates a new skiplist segment
func (b *Builder) NewSegment() *Segment {
	seg := &Segment{tail: make([]*Node, MaxLevel+1),
		head: make([]*Node, MaxLevel+1), builder: b,
		rand: rand.New(rand.NewSource(int64(rand.Int()))),
	}

	seg.sts.IsLocal(true)
	return seg
}

// Assemble multiple skiplist segments and form a parent skiplist
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

// NewBuilder creates a builder based on default config
func NewBuilder() *Builder {
	return NewBuilderWithConfig(DefaultConfig())
}

// NewBuilderWithConfig creates a builder from a config
func NewBuilderWithConfig(cfg Config) *Builder {
	return &Builder{store: NewWithConfig(cfg)}
}
