// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package plasma

import (
	"github.com/couchbase/nitro/skiplist"
	"runtime"
	"unsafe"
)

type smrType int8

const (
	smrPage smrType = iota
	smrPageId
)

var (
	smrChanBufSize         = runtime.NumCPU()
	writerSMRBufferSize    = 50
	swapperSMRInterval     = 20
	lssCleanerSMRInterval  = 20
	pageVisitorSMRInterval = 100
)

type reclaimObject struct {
	typ  smrType
	size uint32
	ptr  unsafe.Pointer
}

type TxToken *skiplist.BarrierSession

func (s *wCtx) HoldLSS() {
	if s.lss != nil {
		s.safeOffset = s.lss.HeadOffset()
	}
}

func (s *wCtx) UnHoldLSS() {
	s.safeOffset = expiredLSSOffset
}

func (s *wCtx) BeginTx() TxToken {
	s.tryThrottleForMemory()
	s.HoldLSS()
	return TxToken(s.Skiplist.GetAccesBarrier().Acquire())
}

func (s *wCtx) EndTx(t TxToken) {
	s.UnHoldLSS()
	s.Skiplist.GetAccesBarrier().Release(t)
}

func (s *Plasma) FreeObjects(lists [][]reclaimObject) {
	if len(lists) > 0 {
		s.Skiplist.GetAccesBarrier().FlushSession(unsafe.Pointer(&lists))
	}
}

func (s *Plasma) newBSDestroyCallback() skiplist.BarrierSessionDestructor {
	return func(ref unsafe.Pointer) {
		s.smrChan <- ref
	}
}

func (s *Plasma) smrWorker(ctx *wCtx) {
	for ptr := range s.smrChan {
		reclaimSet := (*[][]reclaimObject)(ptr)
		for _, reclaimList := range *reclaimSet {
			for _, obj := range reclaimList {
				switch obj.typ {
				case smrPage:
					s.destroyPg((*pageDelta)(obj.ptr))
					ctx.sts.ReclaimSz += int64(obj.size)
				case smrPageId:
					s.FreePageId(PageId((*skiplist.Node)(obj.ptr)), ctx)
					ctx.sts.ReclaimSzIndex += int64(obj.size)
				default:
					panic(obj.typ)
				}
			}
		}
	}

	s.smrWg.Done()
}

func (s *Plasma) destroyAllObjects() {
	count := 1
	buf := s.Skiplist.MakeBuf()
	iter := s.Skiplist.NewIterator(s.cmp, buf)
	defer iter.Close()
	var lastNode *skiplist.Node

	iter.SeekFirst()
	if iter.Valid() {
		lastNode = iter.GetNode()
		iter.Next()
	}

	for lastNode != nil {
		s.freeMM(lastNode.Item())
		s.destroyPg((*pageDelta)(lastNode.Link))
		s.freeMM(unsafe.Pointer(lastNode))
		lastNode = nil
		count++

		if iter.Valid() {
			lastNode = iter.GetNode()
			iter.Next()
		}
	}

	head := s.Skiplist.HeadNode()
	s.destroyPg((*pageDelta)(head.Link))
	tail := s.Skiplist.TailNode()
	s.freeMM(unsafe.Pointer(head))
	s.freeMM(unsafe.Pointer(tail))
}

func (s *Plasma) trySMRObjects(ctx *wCtx, numObjects int) {
	if len(ctx.reclaimList) > numObjects {
		s.FreeObjects([][]reclaimObject{ctx.reclaimList})
		ctx.reclaimList = nil
	}
}

func (s *Plasma) findSafeLSSTrimOffset() LSSOffset {
	minOffset := s.lss.HeadOffset()
	for w := s.wCtxList; w != nil; w = w.next {
		off := w.safeOffset
		if off < expiredLSSOffset && off < minOffset {
			minOffset = off
		}
	}

	return minOffset
}
