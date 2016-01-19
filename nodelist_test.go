package memdb

import (
	"fmt"
	"testing"
)

func TestNodeList(t *testing.T) {
	db := New()
	defer db.Close()

	n := 10
	var list *NodeList
	w := db.NewWriter()
	for i := 0; i < n; i++ {
		ptr := w.Put2([]byte(fmt.Sprintf("%010d", i)))
		if list == nil {
			list = NewNodeList(ptr)
		} else {
			list.Add(ptr)
		}
	}

	count := 0
	for i, k := range list.Keys() {
		expected := fmt.Sprintf("%010d", n-i-1)
		if expected != string(k) {
			t.Errorf("Expected %s, got %s", expected, string(k))
		}
		count++
	}

	if count != n {
		t.Errorf("Expected %d, got %d", n, count)
	}

	list.Remove([]byte(fmt.Sprintf("%010d", 2)))
	list.Remove([]byte(fmt.Sprintf("%010d", 5)))
	list.Remove([]byte(fmt.Sprintf("%010d", 8)))

	count = len(list.Keys())
	if count != n-3 {
		t.Errorf("Expected %d, got %d", n-3, count)
	}

	for i := 10; i < 13; i++ {
		ptr := w.Put2([]byte(fmt.Sprintf("%010d", i)))
		list.Add(ptr)
	}

	count = len(list.Keys())
	if count != n {
		t.Errorf("Expected %d, got %d", n, count)
	}
}
