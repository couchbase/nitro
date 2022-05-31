// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package mm

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"
)

func TestMalloc(t *testing.T) {
	Malloc(100 * 1024 * 1024)
	fmt.Println("size:", Size())
	fmt.Println(Stats())

	stsJsonStr := StatsJson()
	unmarshaledSts := new(map[string]interface{})

	if err := json.Unmarshal([]byte(stsJsonStr), unmarshaledSts); err != nil {
		t.Errorf("Failed to unmarshal json stats: %v", err)
	}

	buf, err := json.MarshalIndent(unmarshaledSts, "", "    ")
	if err != nil {
		t.Errorf("Failed to marshal again: %v", err)
	}
	fmt.Println(string(buf))
}

func TestSizeAt(t *testing.T) {
	p := Malloc(89)
	if sz := SizeAt(p); sz != 96 {
		t.Errorf("Expected sizeclass 96, but got %d", sz)
	}
}

func TestProf(t *testing.T) {
	profPath := "TestProf.prof"

	if err := os.Remove(profPath); err != nil && !os.IsNotExist(err) {
		t.Errorf("Could not remove old profile: err[%v]", err)
		return
	}

	if err := ProfActivate(); err != nil {
		t.Error(err)
		return
	}

	defer func() {
		if err := ProfDeactivate(); err != nil {
			t.Error(err)
		}
	}()

	if err := ProfDump(profPath); err != nil {
		t.Error(err)
		return
	}
}
