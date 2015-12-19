package skiplist

import "testing"
import "fmt"

func TestMerger(t *testing.T) {
	var lists []*Skiplist
	var iters []*Iterator

	s := New()
	cmp := CompareBytes
	buf := s.MakeBuf()
	defer s.FreeBuf(buf)

	n := 5

	for i := 0; i < n; i++ {
		lists = append(lists, New())
	}

	for i := 0; i < 10000; i++ {
		if i >= 1000 && i <= 8000 && i%n == 0 {
			continue
		}
		s := lists[i%n]
		s.Insert(NewByteKeyItem([]byte(fmt.Sprintf("%010d", i))), cmp, buf)
	}

	for i := 0; i < n; i++ {
		buf := s.MakeBuf()
		iters = append(iters, lists[i].NewIterator(cmp, buf))
	}

	mit := NewMergeIterator(iters)

	i := 0
	for mit.SeekFirst(); mit.Valid(); i++ {
		if i >= 1000 && i <= 8000 && i%n == 0 {
			continue
		}
		expected := fmt.Sprintf("%010d", i)
		got := string(*((*byteKeyItem)(mit.Get())))
		if got != expected {
			t.Errorf("Expected %s, got %v", expected, got)
		}
		mit.Next()
	}
}
