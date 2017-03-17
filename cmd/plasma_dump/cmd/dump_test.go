// Copyright © 2017 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/couchbase/nitro/plasma"
)

var ITEM_COUNT = 20000
var dirName = "/tmp/ramdisk2g/testDumpStore"

func setup(t *testing.T, createDir bool) (d string) {
	d = dirName
	os.RemoveAll(d)
	if createDir {
		os.Mkdir(d, 0777)
	}
	cfg := plasma.DefaultConfig()
	cfg.File = d
	s, err := plasma.New(cfg)
	if err != nil || s == nil {
		t.Errorf("Expected plasma open to work!")
	}

	w := s.NewWriter()
	for i := 0; i < ITEM_COUNT; i++ {
		k := []byte(fmt.Sprintf("key%05d", i))
		v := []byte(fmt.Sprintf("val%d", i))
		w.InsertKV(k, v)
	}

	s.Close()

	return
}

func cleanup(dir string) {
	if dir != "" {
		defer os.RemoveAll(dir)
	}
}

func dumpHelper(t *testing.T) (output string) {
	old := os.Stdout // keep backup of the real stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	outC := make(chan string)

	// copy the output in a separate goroutine so dump won't block indefinitely
	go func() {
		var buf bytes.Buffer
		io.Copy(&buf, r)
		outC <- buf.String()
	}()

	dir := setup(t, true)

	dirs := []string{dir}
	err := invokeDump(dirs)
	if err != nil {
		t.Error(err)
	}

	w.Close()
	os.Stdout = old // restoring the real stdout
	output = <-outC

	cleanup(dir)

	return output
}

func TestDump(t *testing.T) {
	out := dumpHelper(t)

	var m []interface{}
	json.Unmarshal([]byte(out), &m)
	if len(m) != 1 {
		t.Errorf("Expected one directory, but count: %d!", len(m))
	}
	store_data, ok := m[0].(map[string]interface{})
	if !ok {
		t.Fatalf("JSON Unmarshal failed")
	}

	kvs := store_data[dirName].([]interface{})

	for i := 0; i < len(kvs); i++ {
		entry := kvs[i].(map[string]interface{})
		k := fmt.Sprintf("key%05d", i)
		v := fmt.Sprintf("val%d", i)
		if strings.Compare(k, entry["k"].(string)) != 0 {
			t.Errorf("Mismatch in key [%s != %s]!", k, entry["k"].(string))
		}
		if strings.Compare(v, entry["v"].(string)) != 0 {
			t.Errorf("Mismatch in value [%s != %s]!", v, entry["v"].(string))
		}
	}
}
