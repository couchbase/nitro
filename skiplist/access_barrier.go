package skiplist

import (
	"math"
	"sync/atomic"
	"unsafe"
)

/*
* Algorithm:
* Access barrier is used to facilitize safe remory reclaimation in the lockfree
* skiplist. Every skiplist access needs to be passed through a gate which tracks
* the safety premitives to figure out when is the right time to deallocate a
* skiplist node.
*
* Even though lockfree skiplist deletion algorithm takes care of completely unlinking
* a skiplist node from the skiplist, still there could be a small perioid during which
* deleted node is accessible to already live skiplist accessors. We need to wait until
* a safe period before the memory for the node can be deallocated.
*
* In this algorithm, the unit of safety period is called barrier session. All the
* live accessor of the skiplist are tracked in a barrier session. Whenever a
* skiplist delete or group of deletes is performed, current barrier session is
* terminator or flushed and new barrier session is started. The previous barrier
* session tracks all the live accessors until the session of terminated. The right
* time to safely reclaim the node is when all the accessor becomes dead. This makes
* sure that unlinked node will be invisible to everyone. The accessors in the
* barrier session can cooperatively detect and mark when each of them terminates.
* When the last accessor terminates, it can take the action to call the destructor
* for the node.
*
* Terminating and installing a new barrier session:
* A session liveCount is incremented everytime an accessor is entering the skiplist
* and decremented when the leave the skiplist. When a session is terminated and new
* one needs to be installed, we just swap the global barrier session reference.
* There could be race conditions while a session is being marked as terminated. Still
* an ongoing skiplist accessor can increment the counter of a session which was marked
* as terminated. To detect those accessors and make them retry, we add a large number
* to the liveCount as part of the session termination phase. When the accessor finds
* that the incremented result is greater than that large offset, it needs to backoff
* from the current session and acquire new session to increment the count. In this
* scheme, whoever decrements the count and gets the count equal to the large offset
* is responsible for deallocation of the object.
* */

type BarrierSessionDestructor func(objectRef unsafe.Pointer)

const barrierFlushOffset = math.MaxInt32 / 2

type BarrierSession struct {
	liveCount *int32
	objectRef unsafe.Pointer
}

func newBarrierSession() *BarrierSession {
	bs := &BarrierSession{
		liveCount: new(int32),
	}

	return bs
}

type AccessBarrier struct {
	session unsafe.Pointer
	callb   BarrierSessionDestructor

	active bool
}

func newAccessBarrier(active bool, callb BarrierSessionDestructor) *AccessBarrier {
	return &AccessBarrier{
		active:  active,
		session: unsafe.Pointer(newBarrierSession()),
		callb:   callb,
	}
}

func (ab *AccessBarrier) Acquire() *BarrierSession {
	if ab.active {
	retry:
		bs := (*BarrierSession)(atomic.LoadPointer(&ab.session))
		liveCount := atomic.AddInt32(bs.liveCount, 1)
		if liveCount > barrierFlushOffset {
			ab.Release(bs)
			goto retry
		}

		return bs
	}

	return nil
}

func (ab *AccessBarrier) Release(bs *BarrierSession) {
	if ab.active {
		liveCount := atomic.AddInt32(bs.liveCount, -1)
		if liveCount == barrierFlushOffset {
			objectRef := bs.objectRef
			if objectRef != nil && atomic.CompareAndSwapPointer(&bs.objectRef, objectRef, nil) {
				ab.callb(objectRef)
			}
		}
	}
}

func (ab *AccessBarrier) FlushSession(ref unsafe.Pointer) {
	if ab.active {
	retry:
		bsPtr := atomic.LoadPointer(&ab.session)
		newBsPtr := unsafe.Pointer(newBarrierSession())
		if atomic.CompareAndSwapPointer(&ab.session, bsPtr, newBsPtr) {
			bs := (*BarrierSession)(bsPtr)
			bs.objectRef = ref

			atomic.AddInt32(bs.liveCount, barrierFlushOffset+1)
			ab.Release(bs)
		} else {
			goto retry
		}
	}
}
