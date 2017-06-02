// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package plasma

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var logTestDataPath = "/tmp/logdir"
var syncMode = true

func TestLogOperation(t *testing.T) {
	os.RemoveAll(logTestDataPath)
	l, _ := newLog(logTestDataPath, 1024*1024, syncMode, false)
	bs := make([]byte, 973)
	n := 1024 * 20
	for i := 0; i < n; i++ {
		copy(bs, []byte(fmt.Sprintf("hello %05d", i)))
		copy(bs[973-5:], []byte(fmt.Sprintf("%05d", i)))
		l.Append(bs)
	}

	l.Commit()

	bs2 := make([]byte, 973)
	for i := 0; i < n; i++ {
		copy(bs, []byte(fmt.Sprintf("hello %05d", i)))
		copy(bs[973-5:], []byte(fmt.Sprintf("%05d", i)))

		l.Read(bs2, int64(i*973))

		if !bytes.Equal(bs, bs2) {
			t.Errorf("Got invalid item for %d", i)
		}
	}

	l.Close()

	l, _ = newLog(logTestDataPath, 1024*1024, syncMode, false)

	for i := 0; i < n; i++ {
		copy(bs, []byte(fmt.Sprintf("hello %05d", i)))
		copy(bs[973-5:], []byte(fmt.Sprintf("%05d", i)))

		l.Read(bs2, int64(i*973))

		if !bytes.Equal(bs, bs2) {
			t.Errorf("Got invalid item for %d", i)
		}
	}
}

func TestLogLargeSize(t *testing.T) {
	os.RemoveAll(logTestDataPath)
	l, _ := newLog(logTestDataPath, 1024*10, syncMode, false)
	bs := make([]byte, 1024*1024)
	for i, _ := range bs {
		bs[i] = 1
	}
	l.Append(bs)
	l.Commit()

	bs = make([]byte, 1024*1024)
	l.Read(bs, 0)
	for i, _ := range bs {
		if bs[i] != 1 {
			t.Errorf("got invalid %d", bs[i])
		}
	}
}

func TestLogTrim(t *testing.T) {
	os.RemoveAll(logTestDataPath)
	l, _ := newLog(logTestDataPath, 1024*1024, syncMode, false)
	bs := make([]byte, 973)
	bs2 := make([]byte, 973)
	n := 1024 * 20
	for i := 0; i < n; i++ {
		copy(bs, []byte(fmt.Sprintf("hello %05d", i)))
		copy(bs[973-5:], []byte(fmt.Sprintf("%05d", i)))
		l.Append(bs)
	}

	l.Trim(973 * 1024 * 10)
	l.Commit()
	l.Close()

	l, _ = newLog(logTestDataPath, 1024*1024, syncMode, false)
	l.Commit()

	for i := 1024 * 10; i < n; i++ {
		copy(bs, []byte(fmt.Sprintf("hello %05d", i)))
		copy(bs[973-5:], []byte(fmt.Sprintf("%05d", i)))

		l.Read(bs2, int64(i*973))

		if !bytes.Equal(bs, bs2) {
			t.Errorf("Got invalid item for %d", i)
		}
	}
}

func TestLogSuperblockCorruption(t *testing.T) {
	os.RemoveAll(logTestDataPath)
	l, _ := newLog(logTestDataPath, 1024*1024, syncMode, false)
	bs := make([]byte, 973)
	n := 1024 * 20
	for i := 0; i < n/2; i++ {
		copy(bs, []byte(fmt.Sprintf("hello %05d", i)))
		copy(bs[973-5:], []byte(fmt.Sprintf("%05d", i)))
		l.Append(bs)
	}

	l.Trim(973 * 1024 * 5)
	l.Commit()
	ho, to := l.Head(), l.Tail()

	for i := n / 2; i < n; i++ {
		copy(bs, []byte(fmt.Sprintf("hello %05d", i)))
		copy(bs[973-5:], []byte(fmt.Sprintf("%05d", i)))
		l.Append(bs)
	}
	l.Commit()
	l.Close()

	gen := 2
	header := filepath.Join(logTestDataPath, "header.data")
	if w, err := os.OpenFile(header, os.O_WRONLY, 0755); err != nil {
		panic(err)
	} else {
		w.WriteAt([]byte("corrupt"), superBlockSize*int64(gen%2))
		w.Close()
	}

	l, err := newLog(logTestDataPath, 1024*1024, syncMode, false)
	if err != nil {
		panic(err)
	}

	if l.Head() != ho {
		t.Errorf("Expected head %d, got %d", ho, l.Head())
	}

	if l.Tail() != to {
		t.Errorf("Expected tail %d, got %d", to, l.Tail())
	}
}

func TestLogTrimHolePunch(t *testing.T) {
	os.RemoveAll(logTestDataPath)

	rbSz := reclaimBlockSize
	sHP := supportedHolePunch
	defer func() {
		reclaimBlockSize = rbSz
		supportedHolePunch = sHP
	}()

	reclaimBlockSize = 1024 * 4
	supportedHolePunch = true
	l, _ := newLog(logTestDataPath, 1024*1024, syncMode, false)
	bs := make([]byte, 1024)
	n := 1024 * 10
	for i := 0; i < n; i++ {
		l.Append(bs)
	}
	l.Commit()

	sz := l.Size()
	if sz != int64(n*len(bs)) {
		t.Errorf("unexpected size %d", sz)
	}

	for i := 0; i < n; i++ {
		off := int64(i * 1024)
		l.Trim(off)
		l.Commit()

		expected := (off / reclaimBlockSize) * reclaimBlockSize
		if expected != sz-l.Size() {
			t.Errorf("Expected reclaimed space = %d, got %d", expected, sz-l.Size())
		}
	}

	l.Close()
}
