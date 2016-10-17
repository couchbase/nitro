package plasma

// Naive delta merger implementation
import (
	"unsafe"
)

func (s *pageItemSorter) Add(itms ...PageItem) {
	s.itms = append(s.itms, itms...)
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

func (pg *page) collectPageItems(head *pageDelta,
	loItm, hiItm unsafe.Pointer) (items []PageItem, dataSz int) {
	sorter := pg.newPageItemSorter(head)

	hasReloc := false
	for pd := head; pd != nil; pd = pd.next {
		switch pd.op {
		case opInsertDelta, opDeleteDelta:
			rec := (*recordDelta)(unsafe.Pointer(pd))
			if pg.inRange(loItm, hiItm, rec.itm) {
				sorter.Add(rec)
			}
		case opPageSplitDelta:
			pds := (*splitPageDelta)(unsafe.Pointer(pd))
			hiItm = pds.hiItm
		case opPageMergeDelta:
			pdm := (*mergePageDelta)(unsafe.Pointer(pd))
			items, fdSz := pg.collectPageItems(pdm.mergeSibling, loItm, hiItm)
			if !hasReloc {
				dataSz += fdSz
			}
			sorter.Add(items...)
		case opBasePage:
			bp := (*basePage)(unsafe.Pointer(pd))
			var pgItms []PageItem

			for _, itm := range bp.items {
				if pg.inRange(loItm, hiItm, itm) {
					pgItms = append(pgItms, &pageItem{itm: itm})
				}
			}

			merger := pg.newPageItemSorter(nil)
			merger.Init(pgItms)
			return merger.Merge(sorter.Run()), dataSz
		case opFlushPageDelta:
			if !hasReloc {
				fpd := (*flushPageDelta)(unsafe.Pointer(pd))
				dataSz += int(fpd.flushDataSz)
			}
		case opRelocPageDelta:
			fpd := (*flushPageDelta)(unsafe.Pointer(pd))
			if !hasReloc {
				dataSz += int(fpd.flushDataSz)
				hasReloc = true
			}
		}
	}

	return sorter.Run(), dataSz
}

func (pg *page) collectItems2(head *pageDelta,
	loItm, hiItm unsafe.Pointer) (itx []unsafe.Pointer, dataSz int) {
	var itms []unsafe.Pointer
	items, dataSz := pg.collectPageItems(head, loItm, hiItm)
	for _, itm := range items {
		if itm.IsInsert() {
			itms = append(itms, itm.Item())
		}
	}

	return itms, dataSz
}

func (pg *page) newPageItemSorter(head *pageDelta) pageItemSorter {
	chainLen := 0
	if head != nil {
		chainLen = int(head.chainLen)
	}

	return pageItemSorter{
		cmp:  pg.cmp,
		itms: make([]PageItem, 0, chainLen),
	}
}
