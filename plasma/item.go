package plasma

import (
	"bytes"
	"fmt"
	"github.com/couchbase/nitro/skiplist"
	"reflect"
	"unsafe"
)

// TODO: Cleanup the current ugly-hackup implementation

// Layout for the item is as follows:
// [    32 bit header                    ][opt 32 bit keylen][opt key][64 bit sn][opt val]
// [insert bit][val bit][ptr key bit][len]

const (
	itmInsertFlag = 0x80000000
	itmHasValFlag = 0x40000000
	itmPtrKeyFlag = 0x20000000
	itmLenMask    = 0x1fffffff
	itmHdrLen     = 4
	itmSnSize     = 8
	itmKlenSize   = 4
)

const (
	ptrKeyMarker = uintptr((uint64(1) << 63))
	itmPtrMask   = ^ptrKeyMarker
)

// A placeholder type for holding item data
type item uint32

func (itm *item) Size() int {
	sz := itm.l() + itmHdrLen + itmSnSize
	if *itm&itmPtrKeyFlag > 0 {
		itm = itm.getPtrKeyItem()
		_, klen := itm.k()
		sz += klen
	}

	return sz
}

func (itm *item) ActualSize() int {
	return itm.l() + itmHdrLen + itmSnSize
}

func (itm *item) IsInsert() bool {
	return itmInsertFlag&*itm > 0

}

func (itm *item) Item() unsafe.Pointer {
	return unsafe.Pointer(itm)
}

func (itm *item) Len() int {
	return 1
}

func (itm *item) At(int) PageItem {
	return itm
}

func (itm *item) HasValue() bool {
	return itmHasValFlag&*itm > 0
}

func (itm *item) Sn() uint64 {
	kptr, klen := itm.k()
	return *(*uint64)(unsafe.Pointer(kptr + uintptr(klen)))
}

func (itm *item) l() int {
	return int(itmLenMask & *itm)
}

func (itm *item) k() (uintptr, int) {
	basePtr := uintptr(unsafe.Pointer(itm)) + itmHdrLen
	var klen int
	var kptr uintptr

	if itm.HasValue() {
		klen = int(*(*uint32)(unsafe.Pointer(basePtr)))
		kptr = basePtr + itmKlenSize
	} else {
		klen = itm.l()
		kptr = basePtr
	}

	return kptr, klen
}

func (itm *item) getPtrKeyItem() *item {
	for *itm&itmPtrKeyFlag > 0 {
		itm = (*item)(unsafe.Pointer(uintptr(unsafe.Pointer(itm)) + uintptr(itm.ActualSize())))
	}

	return itm
}

func (itm *item) Key() (bs []byte) {
	itm = itm.getPtrKeyItem()
	kptr, klen := itm.k()

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	sh.Data = kptr
	sh.Len = klen
	sh.Cap = klen
	return
}

func (itm *item) Value() (bs []byte) {
	kptr, klen := itm.k()
	l := itm.l()

	sh := (*reflect.SliceHeader)(unsafe.Pointer(&bs))
	sh.Data = kptr + uintptr(klen) + itmSnSize
	sh.Len = l - klen - itmKlenSize
	sh.Cap = sh.Len
	return
}

func newItem(k, v []byte, sn uint64, del bool, buf *Buffer) (
	*item, error) {
	if len(k) > itmLenMask {
		return nil, ErrKeyTooLarge
	}

	kl := len(k)
	vl := len(v)

	sz := uintptr(itmHdrLen + itmSnSize + kl + vl)
	if vl > 0 {
		sz += itmKlenSize
	}

	var ptr unsafe.Pointer
	if buf == nil {
		b := make([]byte, sz)
		ptr = unsafe.Pointer(&b[0])
	} else {
		buf.Grow(0, int(sz))
		ptr = buf.Ptr(0)
	}
	newItem2(k, v, sn, del, false, ptr)
	return (*item)(ptr), nil
}

func newItem2(k, v []byte, sn uint64, del bool, ptrKey bool, ptr unsafe.Pointer) {

	kl := len(k)
	vl := len(v)

	hdr := (*uint32)(ptr)
	*hdr = 0
	if !del {
		*hdr |= itmInsertFlag
	}

	if ptrKey {
		*hdr |= itmPtrKeyFlag
	}

	if vl > 0 {
		*hdr |= itmHasValFlag | uint32(vl+kl+itmKlenSize)
		klen := (*uint32)(unsafe.Pointer(uintptr(ptr) + itmHdrLen))
		*klen = uint32(kl)

		snp := (*uint64)(unsafe.Pointer(uintptr(ptr) + uintptr(itmHdrLen+itmKlenSize+kl)))
		*snp = sn
		if kl > 0 {
			memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen+itmKlenSize), unsafe.Pointer(&k[0]), kl)
		}
		memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen+itmKlenSize+itmSnSize+uintptr(kl)), unsafe.Pointer(&v[0]), vl)
	} else {
		snp := (*uint64)(unsafe.Pointer(uintptr(ptr) + uintptr(itmHdrLen+kl)))
		*snp = sn
		*hdr |= uint32(kl)
		if kl > 0 {
			memcopy(unsafe.Pointer(uintptr(ptr)+itmHdrLen), unsafe.Pointer(&k[0]), kl)
		}
	}
}

func cmpItem(a, b unsafe.Pointer) int {
	if a == skiplist.MinItem || b == skiplist.MaxItem {
		return -1
	}

	if a == skiplist.MaxItem || b == skiplist.MinItem {
		return 1
	}

	itma := (*item)(a)
	itmb := (*item)(b)

	return bytes.Compare(itma.Key(), itmb.Key())
}

func itemStringer(itm unsafe.Pointer) string {
	if itm == skiplist.MinItem {
		return "minItem"
	} else if itm == skiplist.MaxItem {
		return "maxItem"
	}

	x := (*item)(itm)
	v := "(nil)"
	if x.HasValue() {
		v = string(x.Value())
	}
	return fmt.Sprintf("item key:%s val:%s sn:%d insert: %v", string(x.Key()), v, x.Sn(), x.IsInsert())
}

func copyItem(a, b unsafe.Pointer, sz int) {
	if *(*item)(b)&itmPtrKeyFlag > 0 {
		copyPtrKeyItem(a, b)
	} else {
		memcopy(a, b, sz)
	}
}

func encodePtrKeyItem(dstItm, srcItm unsafe.Pointer) {
	var v []byte
	itm := (*item)(srcItm)
	if itm.HasValue() {
		v = itm.Value()
	}
	sn := itm.Sn()
	del := !itm.IsInsert()

	newItem2(nil, v, sn, del, true, dstItm)
}

func copyPtrKeyItem(dstItm, srcItm unsafe.Pointer) {
	var v []byte
	itm := (*item)(srcItm)
	k := itm.Key()
	if itm.HasValue() {
		v = itm.Value()
	}
	sn := itm.Sn()
	del := !itm.IsInsert()

	newItem2(k, v, sn, del, false, dstItm)
}

func copyItemRun(srcItms, dstItms []unsafe.Pointer, data unsafe.Pointer) {
	var offset uintptr
	for i, itm := range srcItms {
		dstItm := unsafe.Pointer(uintptr(data) + offset)
		srcItm := unsafe.Pointer(uintptr(itm) & itmPtrMask)
		usePtrKey := uintptr(itm)&ptrKeyMarker > 0
		if usePtrKey {
			encodePtrKeyItem(dstItm, srcItm)
			sz := (*item)(dstItm).ActualSize()
			offset += uintptr(sz)
		} else {
			sz := (*item)(srcItm).Size()
			copyItem(dstItm, srcItm, sz)
			offset += uintptr(sz)
		}
		dstItms[i] = dstItm
	}
}

func itemRunSize(itms []unsafe.Pointer) uintptr {
	var lastKey []byte
	var sz uintptr
	for i := len(itms) - 1; i >= 0; i-- {
		sz += uintptr((*item)(itms[i]).Size())
		currKey := (*item)(itms[i]).Key()
		if lastKey != nil {
			if bytes.Equal(currKey, lastKey) {
				sz -= uintptr(len(currKey))
				itms[i] = unsafe.Pointer(uintptr(itms[i]) | ptrKeyMarker)
			}
		}

		lastKey = currKey
	}

	return sz
}
