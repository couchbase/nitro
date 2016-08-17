package plasma

import (
	"github.com/t3rm1n4l/nitro/skiplist"
	"testing"
	"unsafe"
)

func newTestIntPlasmaStore() *Plasma {
	cfg := Config{
		MaxDeltaChainLen: 200,
		MaxPageItems:     400,
		MinPageItems:     25,
		Compare:          skiplist.CompareInt,
		ItemSize: func(unsafe.Pointer) uintptr {
			return unsafe.Sizeof(new(skiplist.IntKeyItem))
		},
	}
	return New(cfg)
}

func TestPlasmaSimple(t *testing.T) {
	s := newTestIntPlasmaStore()
	w := s.NewWriter()
	for i := 0; i < 1000000; i++ {
		w.Insert(skiplist.NewIntKeyItem(i))
	}

	for i := 0; i < 1000000; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got := w.Lookup(itm)
		if skiplist.CompareInt(itm, got) != 0 {
			t.Errorf("mismatch %d != %d", i, skiplist.IntFromItem(got))
		}
	}

	for i := 0; i < 800000; i++ {
		w.Delete(skiplist.NewIntKeyItem(i))
	}

	for i := 0; i < 1000000; i++ {
		itm := skiplist.NewIntKeyItem(i)
		got := w.Lookup(itm)
		if i < 800000 {
			if got != nil {
				t.Errorf("Expected missing %d", i)
			}
		} else {
			if skiplist.CompareInt(itm, got) != 0 {
				t.Errorf("Expected %d, got %d", i, skiplist.IntFromItem(got))
			}
		}
	}
}
