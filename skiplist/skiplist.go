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
	"math/rand"
	"runtime"
	"sync/atomic"
	"unsafe"
)

// Debug flag enables additional stats gathering
var Debug bool

// MaxLevel is the limit for the skiplist levels
const MaxLevel = 32
const p = 0.25

// CompareFn is the skiplist item comparator
type CompareFn func(unsafe.Pointer, unsafe.Pointer) int

// ItemSizeFn returns size of a skiplist item
type ItemSizeFn func(unsafe.Pointer) int

func defaultItemSize(unsafe.Pointer) int {
	return 0
}

// MallocFn is a custom memory allocator
type MallocFn func(int) unsafe.Pointer

// FreeFn is a custom memory deallocator
type FreeFn func(unsafe.Pointer)

// Config holds skiplist configuration
type Config struct {
	ItemSize ItemSizeFn

	UseMemoryMgmt     bool
	Malloc            MallocFn
	Free              FreeFn
	BarrierDestructor BarrierSessionDestructor
}

// SetItemSizeFunc configures item size function
func (cfg *Config) SetItemSizeFunc(fn ItemSizeFn) {
	cfg.ItemSize = fn
}

// DefaultConfig returns default skiplist configuration
func DefaultConfig() Config {
	return Config{
		ItemSize:      defaultItemSize,
		UseMemoryMgmt: false,
	}
}

// Skiplist - core data structure
type Skiplist struct {
	head    *Node
	tail    *Node
	level   int32
	Stats   Stats
	barrier *AccessBarrier

	newNode  func(itm unsafe.Pointer, level int) *Node
	freeNode func(*Node)

	Config
}

// New creates a skiplist with default config
func New() *Skiplist {
	return NewWithConfig(DefaultConfig())
}

// NewWithConfig creates a config from given config
func NewWithConfig(cfg Config) *Skiplist {
	if runtime.GOARCH != "amd64" {
		cfg.UseMemoryMgmt = false
	}

	s := &Skiplist{
		Config:  cfg,
		barrier: newAccessBarrier(cfg.UseMemoryMgmt, cfg.BarrierDestructor),
	}

	s.newNode = func(itm unsafe.Pointer, level int) *Node {
		return allocNode(itm, level, cfg.Malloc)
	}

	if cfg.UseMemoryMgmt {
		s.freeNode = func(n *Node) {
			if Debug {
				debugMarkFree(n)
			}
			cfg.Free(unsafe.Pointer(n))
		}
	} else {
		s.freeNode = func(*Node) {}
	}

	head := allocNode(MinItem, MaxLevel, nil)
	tail := allocNode(MaxItem, MaxLevel, nil)

	for i := 0; i <= MaxLevel; i++ {
		head.setNext(i, tail, false)
	}

	s.head = head
	s.tail = tail

	return s
}

// GetAccesBarrier returns current active access barrier
func (s *Skiplist) GetAccesBarrier() *AccessBarrier {
	return s.barrier
}

// FreeNode deallocates the skiplist node memory
func (s *Skiplist) FreeNode(n *Node, sts *Stats) {
	s.freeNode(n)
	sts.AddInt64(&sts.nodeFrees, 1)
}

func (s *Skiplist) NewNode(level int) *Node {
	return s.newNode(nil, level)
}

func (s *Skiplist) HeadNode() *Node {
	return s.head
}

func (s *Skiplist) TailNode() *Node {
	return s.tail
}

// ActionBuffer is a temporary buffer used by skiplist operations
type ActionBuffer struct {
	preds []*Node
	succs []*Node
}

// MakeBuf creates an action buffer
func (s *Skiplist) MakeBuf() *ActionBuffer {
	return &ActionBuffer{
		preds: make([]*Node, MaxLevel+1),
		succs: make([]*Node, MaxLevel+1),
	}
}

// FreeBuf frees an action buffer
func (s *Skiplist) FreeBuf(b *ActionBuffer) {
}

// Size returns the size of a node
func (s *Skiplist) Size(n *Node) int {
	return s.ItemSize(n.Item()) + n.Size()
}

// NewLevel returns a random level for the next node
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

func (s *Skiplist) helpDelete(level int, prev, curr, next *Node, sts *Stats) bool {
	success := prev.dcasNext(level, curr, next, false, false)
	if success && level == 0 {
		sts.AddInt64(&sts.softDeletes, -1)
		sts.AddInt64(&sts.levelNodesCount[curr.Level()], -1)
		sts.AddInt64(&sts.usedBytes, -int64(s.Size(curr)))
	}
	return success
}

func (s *Skiplist) Lookup(itm unsafe.Pointer, cmp CompareFn, buf *ActionBuffer, sts *Stats) (pred *Node, curr *Node, found bool) {
	found = s.findPath(itm, cmp, buf, sts) != nil
	pred = buf.preds[0]
	curr = buf.succs[0]
	return
}

func (s *Skiplist) findPath(itm unsafe.Pointer, cmp CompareFn,
	buf *ActionBuffer, sts *Stats) (foundNode *Node) {
	var cmpVal = 1

retry:
	prev := s.head
	level := int(atomic.LoadInt32(&s.level))
	for i := level; i >= 0; i-- {
		curr, _ := prev.getNext(i)
	levelSearch:
		for {
			next, deleted := curr.getNext(i)
			for deleted {
				if !s.helpDelete(i, prev, curr, next, sts) {
					sts.AddUint64(&sts.readConflicts, 1)
					goto retry
				}

				curr, _ = prev.getNext(i)
				next, deleted = curr.getNext(i)
			}

			cmpVal = compare(cmp, curr.Item(), itm)
			if cmpVal < 0 {
				prev = curr
				curr = next
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

// Insert adds an item into the skiplist
func (s *Skiplist) Insert(itm unsafe.Pointer, cmp CompareFn,
	buf *ActionBuffer, sts *Stats) (success bool) {
	_, success = s.Insert2(itm, cmp, nil, buf, rand.Float32, sts)
	return
}

// Insert2 is a more verbose version of Insert
func (s *Skiplist) Insert2(itm unsafe.Pointer, inscmp CompareFn, eqCmp CompareFn,
	buf *ActionBuffer, randFn func() float32, sts *Stats) (*Node, bool) {
	itemLevel := s.NewLevel(randFn)
	return s.Insert3(itm, inscmp, eqCmp, buf, itemLevel, false, sts)
}

// Insert3 is more verbose version of Insert2
func (s *Skiplist) Insert3(itm unsafe.Pointer, insCmp CompareFn, eqCmp CompareFn,
	buf *ActionBuffer, itemLevel int, skipFindPath bool, sts *Stats) (*Node, bool) {

	token := s.barrier.Acquire()
	defer s.barrier.Release(token)

	x := s.newNode(itm, itemLevel)
	return x, s.Insert4(x, insCmp, eqCmp, buf, itemLevel, skipFindPath, sts)
}

func (s *Skiplist) Insert4(x *Node, insCmp CompareFn, eqCmp CompareFn, buf *ActionBuffer,
	itemLevel int, skipFindPath bool, sts *Stats) bool {

	itm := x.Item()

retry:
	if skipFindPath {
		skipFindPath = false
	} else {
		if s.findPath(itm, insCmp, buf, sts) != nil ||
			eqCmp != nil && compare(eqCmp, itm, buf.preds[0].Item()) == 0 {

			s.freeNode(x)
			return false
		}
	}

	// Set all next links for the node non-atomically
	for i := 0; i <= int(itemLevel); i++ {
		x.setNext(i, buf.succs[i], false)
	}

	// Now node is part of the skiplist
	if !buf.preds[0].dcasNext(0, buf.succs[0], x, false, false) {
		sts.AddUint64(&sts.insertConflicts, 1)
		goto retry
	}

	// Add to index levels
	for i := 1; i <= int(itemLevel); i++ {
	fixThisLevel:
		for {
			nodeNext, deleted := x.getNext(i)
			next := buf.succs[i]

			// Update the node's next pointer at current level if required.
			// This is the only thread which can modify next pointer at this level
			// The dcas operation can fail only if another thread marked delete
			if deleted || (nodeNext != next && !x.dcasNext(i, nodeNext, next, false, false)) {
				goto finished
			}

			if buf.preds[i].dcasNext(i, next, x, false, false) {
				break fixThisLevel
			}

			s.findPath(itm, insCmp, buf, sts)
		}
	}

finished:
	sts.AddInt64(&sts.nodeAllocs, 1)
	sts.AddInt64(&sts.levelNodesCount[itemLevel], 1)
	sts.AddInt64(&sts.usedBytes, int64(s.Size(x)))
	return true
}

func (s *Skiplist) softDelete(delNode *Node, sts *Stats) bool {
	var marked bool

	targetLevel := delNode.Level()
	for i := targetLevel; i >= 0; i-- {
		next, deleted := delNode.getNext(i)
		for !deleted {
			if delNode.dcasNext(i, next, next, false, true) && i == 0 {
				sts.AddInt64(&sts.softDeletes, 1)
				marked = true
			}
			next, deleted = delNode.getNext(i)
		}
	}
	return marked
}

// Delete an item from the skiplist
func (s *Skiplist) Delete(itm unsafe.Pointer, cmp CompareFn,
	buf *ActionBuffer, sts *Stats) bool {
	token := s.barrier.Acquire()
	defer s.barrier.Release(token)

	found := s.findPath(itm, cmp, buf, sts) != nil
	if !found {
		return false
	}

	delNode := buf.succs[0]
	return s.deleteNode(delNode, cmp, buf, sts)
}

// DeleteNode an item from the skiplist by specifying its node
func (s *Skiplist) DeleteNode(n *Node, cmp CompareFn,
	buf *ActionBuffer, sts *Stats) bool {
	token := s.barrier.Acquire()
	defer s.barrier.Release(token)

	return s.deleteNode(n, cmp, buf, sts)
}

func (s *Skiplist) deleteNode(n *Node, cmp CompareFn, buf *ActionBuffer, sts *Stats) bool {
	itm := n.Item()
	if s.softDelete(n, sts) {
		s.findPath(itm, cmp, buf, sts)
		return true
	}

	return false
}

// GetRangeSplitItems returns `nways` split range pivots of the skiplist items
// Explicit barrier and release should be used by the caller before
// and after this function call
func (s *Skiplist) GetRangeSplitItems(nways int) []unsafe.Pointer {
	var deleted bool
repeat:
	var itms []unsafe.Pointer
	var finished bool

	l := int(atomic.LoadInt32(&s.level))
	for ; l >= 0; l-- {
		c := int(atomic.LoadInt64(&s.Stats.levelNodesCount[l]) + 1)
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
