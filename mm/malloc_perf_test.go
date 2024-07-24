//go:build perf
// +build perf

// Copyright 2024-Present Couchbase, Inc.
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

func BenchmarkArenaMalloc(b *testing.B) {
	CreateArenas()
	defer func() {
		fmt.Println(Stats())
	}()

	sz := 128

	b.Run("Malloc", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Malloc(sz)
		}
	})

	b.Run("MallocArena", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			MallocArena(sz)
		}
	})
}
