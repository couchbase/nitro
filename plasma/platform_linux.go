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
	"io/ioutil"
	"os"
	"syscall"
)

var supportedHolePunch bool

func init() {
	if f, err := ioutil.TempFile("", "test_holepunch"); err == nil {
		if _, err := f.Write(make([]byte, 4096, 4096)); err == nil {
			if punchHole(f, 0, 4096) == nil {
				supportedHolePunch = true
			}
		}

		f.Close()
		os.Remove(f.Name())
	}
}

func punchHole(f *os.File, offset, size int64) error {
	return syscall.Fallocate(int(f.Fd()),
		FALLOC_FL_PUNCH_HOLE|FALLOC_FL_PUNCH_HOLEOC_FL_KEEP_SIZE, offset,
		size)
}
