// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package nitro

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
