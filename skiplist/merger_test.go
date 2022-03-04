// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.
package skiplist

import (
	"fmt"
	"testing"
	"unsafe"
)

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
		s.Insert(NewByteKeyItem([]byte(fmt.Sprintf("%010d", i))), cmp, buf, &s.Stats)
	}

	for i := 0; i < n; i++ {
		buf := s.MakeBuf()
		iters = append(iters, lists[i].NewIterator(cmp, buf))
	}

	mit := NewMergeIterator(iters)
	var seekPtr unsafe.Pointer
	var seekPt int

	i := 0
	for mit.SeekFirst(); mit.Valid(); i++ {
		if i >= 1000 && i <= 8000 && i%n == 0 {
			continue
		}
		expected := fmt.Sprintf("%010d", i)
		seekPtr = mit.Get()
		seekPt = i
		got := string(*((*byteKeyItem)(seekPtr)))
		if got != expected {
			t.Errorf("Expected %s, got %v", expected, got)
		}
		mit.Next()
	}
	ok := mit.Seek(seekPtr)
	if !ok {
		t.Errorf("Expected seek to work")
	}
	seekPtr = mit.Get()
	got := string(*((*byteKeyItem)(seekPtr)))

	expected := fmt.Sprintf("%010d", seekPt)
	if got != expected {
		t.Errorf("Expected %s, got %v", expected, got)
	}
	node := mit.GetNode()
	if node == nil {
		t.Errorf("Expected getNode to work")
	}

}
