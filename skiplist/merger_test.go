// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.
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
		s.Insert(NewByteKeyItem([]byte(fmt.Sprintf("%010d", i))), cmp, buf, &s.Stats)
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
