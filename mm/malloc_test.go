// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

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
