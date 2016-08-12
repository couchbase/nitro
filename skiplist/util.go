package skiplist

import (
	"reflect"
	"sort"
	"unsafe"
)

func memcopy(dst, src unsafe.Pointer, sz int) {
	var sb, db []byte
	hdrsb := (*reflect.SliceHeader)(unsafe.Pointer(&sb))
	hdrsb.Len = sz
	hdrsb.Cap = hdrsb.Len
	hdrsb.Data = uintptr(src)

	hdrdb := (*reflect.SliceHeader)(unsafe.Pointer(&db))
	hdrdb.Len = sz
	hdrdb.Cap = hdrdb.Len
	hdrdb.Data = uintptr(dst)
	copy(db, sb)
}

type pageItemSorter struct {
	itms []PageItem
	cmp  CompareFn
}

func (s *pageItemSorter) Len() int {
	return len(s.itms)
}

func (s *pageItemSorter) Less(i, j int) bool {
	return s.cmp(s.itms[i].Item(), s.itms[j].Item()) < 0
}

func (s *pageItemSorter) Swap(i, j int) {
	s.itms[i], s.itms[j] = s.itms[j], s.itms[i]
}

func (s *pageItemSorter) Add(itms ...PageItem) {
	s.itms = append(s.itms, itms...)
}

func (s *pageItemSorter) Run() []PageItem {
	sort.Stable(s)
	x, y := 0, 1
	for y < s.Len() {
		if s.Less(x, y) {
			x++
			if x != y {
				s.Swap(x, y)
			}
		}
		y++
	}

	if x < s.Len() {
		x++
	}
	return s.itms[:x]
}

func (s *pageItemSorter) Init(itms []PageItem) {
	s.itms = itms
}

func (s *pageItemSorter) Merge(itms []PageItem) []PageItem {
	pgItms := make([]PageItem, 0, len(itms))
	var i, j int
loop:
	for {
		var cmpval int
		if i < len(s.itms) && j < len(itms) {
			cmpval = s.cmp(s.itms[i].Item(), itms[j].Item())
		} else if i < len(s.itms) {
			cmpval = -1
		} else if j < len(itms) {
			cmpval = 1
		} else {
			break loop
		}

		switch {
		case cmpval < 0:
			pgItms = append(pgItms, s.itms[i])
			i++
		case cmpval == 0:
			if itms[j].IsInsert() {
				pgItms = append(pgItms, itms[j])
			}
			i++
			j++
		default:
			pgItms = append(pgItms, itms[j])
			j++
		}
	}

	return pgItms
}
