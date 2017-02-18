package plasma

import (
	"github.com/couchbase/nitro/skiplist"
	"unsafe"
)

type smrType int8

const (
	smrPage smrType = iota
	smrPageId
)

const smrChanBufSize = 256

type reclaimObject struct {
	typ  smrType
	size uint32
	ptr  unsafe.Pointer
}

type TxToken *skiplist.BarrierSession

func (s *Plasma) BeginTx() TxToken {
	return TxToken(s.Skiplist.GetAccesBarrier().Acquire())
}

func (s *Plasma) EndTx(t TxToken) {
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
}

func (s *Plasma) trySMRObjects(ctx *wCtx, numObjects int) {
	if len(ctx.reclaimList) > numObjects {
		s.FreeObjects([][]reclaimObject{ctx.reclaimList})
		ctx.reclaimList = nil
	}
}
