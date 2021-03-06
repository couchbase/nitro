// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package mm

import (
	"fmt"
	"testing"
)

func TestMalloc(t *testing.T) {
	Malloc(100 * 1024 * 1024)
	fmt.Println("size:", Size())
	fmt.Println(Stats())
}

func TestSizeAt(t *testing.T) {
	p := Malloc(89)
	if sz := SizeAt(p); sz != 96 {
		t.Errorf("Expected sizeclass 96, but got %d", sz)
	}
}
