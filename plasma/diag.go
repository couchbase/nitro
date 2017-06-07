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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"unsafe"
)

var Diag diag

type diag struct {
	sync.Mutex
}

type diagRequest struct {
	Cmd  string
	Args []string
}

func (d *diag) Command(cmd string, args ...string) string {
	d.Lock()
	defer d.Unlock()

	getDB := func() *Plasma {
		arg0, _ := strconv.Atoi(args[0])
		db := (*Plasma)(unsafe.Pointer(uintptr(arg0)))
		return db
	}

	getWr := func(db *Plasma) *Writer {
		if db.diagWr == nil {
			db.diagWr = db.NewWriter()
		}

		return db.diagWr
	}

	doPersist := func(evict bool) {
		db := getDB()
		wr := getWr(db)
		callb := func(pid PageId, partn RangePartition) error {
			db.Persist(pid, evict, wr.wCtx)
			return nil
		}

		db.PageVisitor(callb, 1)
	}

	s := fmt.Sprintf("----------plasma-diagnostics---------\n")
	switch cmd {
	case "list":
		s += fmt.Sprintf("Command: list\n")
		for i, db := range d.ListInstances() {
			s += fmt.Sprintf("[%d] %s: %d\n", i, db.logPrefix, uintptr(unsafe.Pointer(db)))
		}

	case "stats":
		db := getDB()
		s += db.GetStats().String()
	case "compactAll":
		wr := getWr(getDB())
		wr.CompactAll()
	case "evictAll":
		doPersist(true)
	case "persistAll":
		doPersist(false)
	default:
		s += fmt.Sprintf("Invalid command: %s\n", cmd)
	}
	s += fmt.Sprintf("-------------------------------------\n")

	return s
}

func (d *diag) ListInstances() []*Plasma {
	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)

	var dbList []*Plasma
	iter := dbInstances.NewIterator(ComparePlasma, buf)
	defer iter.Close()

	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		db := (*Plasma)(iter.Get())
		dbList = append(dbList, db)
	}

	return dbList
}

func (d *diag) HandleHttp(w http.ResponseWriter, r *http.Request) {
	bytes, _ := ioutil.ReadAll(r.Body)
	var req diagRequest
	json.Unmarshal(bytes, &req)
	s := d.Command(req.Cmd, req.Args...)
	w.WriteHeader(200)
	w.Write([]byte(s))
}
