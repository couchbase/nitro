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
	"bufio"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/couchbase/nitro/skiplist"
	"io/ioutil"
	"net/http"
	"sync"
	"unsafe"
)

var Diag diag

type diag struct {
	sync.Mutex
}

type diagRequest struct {
	Cmd  string
	Args []interface{}
}

func (d *diag) Command(cmd string, w *bufio.Writer, args ...interface{}) {
	d.Lock()
	defer d.Unlock()

	defer func() {
		if r := recover(); r != nil {
			w.WriteString(fmt.Sprintf("Error: panic - %v\n", r))
		}
	}()

	getDB := func() *Plasma {
		db := (*Plasma)(unsafe.Pointer(uintptr(int(args[0].(float64)))))
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

	hexCodeKey := func(db *Plasma, itm unsafe.Pointer) string {
		bs := make([]byte, db.IndexKeySize(itm))
		db.CopyIndexKey(unsafe.Pointer(&bs[0]), itm, len(bs))

		buf := make([]byte, hex.EncodedLen(len(bs)))
		hex.Encode(buf, bs)
		return string(buf)
	}

	w.WriteString(fmt.Sprintf("----------plasma-diagnostics---------\n"))
	switch cmd {
	case "listDBs":
		for i, db := range d.ListInstances() {
			s := fmt.Sprintf("[%d] %s: %d\n", i, db.logPrefix, uintptr(unsafe.Pointer(db)))
			w.WriteString(s)
		}
	case "stats":
		db := getDB()
		w.WriteString(db.GetStats().String())
	case "compactAll":
		wr := getWr(getDB())
		wr.CompactAll()
	case "evictAll":
		doPersist(true)
	case "persistAll":
		doPersist(false)
	case "listPids":
		c := -1
		db := getDB()
		callb := func(pid PageId, partn RangePartition) error {
			c++
			n := pid.(*skiplist.Node)
			itm := n.Item()
			if itm == skiplist.MinItem {
				w.WriteString(fmt.Sprintf("[%d] \"\" | -inf\n", c))
				return nil
			}

			key := hexCodeKey(db, itm)
			w.WriteString(fmt.Sprintf("[%d] %s | %s\n", c, key, itemStringer(itm)))
			return nil
		}

		db.PageVisitor(callb, 1)
	case "dumpPage":
		db := getDB()
		wr := getWr(db)
		tx := wr.BeginTx()
		defer wr.EndTx(tx)

		var pidItem unsafe.Pointer
		if len(args[1].(string)) > 0 {

			bs, err := hex.DecodeString(args[1].(string))
			if err != nil {
				w.WriteString(fmt.Sprintf("Error: Invalid pageId key %s\n", args[1]))
				return
			}
			pidItem = unsafe.Pointer(&bs[0])
		}

		pid := db.getPageId(pidItem, wr.wCtx)
		if pid == nil {
			w.WriteString(fmt.Sprintf("Error: PageId key %s not found\n", args[1]))
			return
		}

		pg, err := db.ReadPage(pid, nil, false, wr.wCtx)
		if err != nil {
			w.WriteString(fmt.Sprintf("Error: pageRead err %v", err))
			return
		}
		w.WriteString(prettyPrint(pg.(*page).head, itemStringer, wr.wCtx))
		w.WriteString(fmt.Sprintln("memory_used:", pg.ComputeMemUsed()))
	case "memoryUsage":
		c := -1
		db := getDB()
		wr := getWr(db)

		callb := func(pid PageId, partn RangePartition) error {
			c++
			n := pid.(*skiplist.Node)
			pg, err := db.ReadPage(pid, nil, false, wr.wCtx)
			if err != nil {
				w.WriteString(fmt.Sprintf("Error: pageRead err %v", err))
				return fmt.Errorf("error")
			}

			deltaLen := pg.(*page).head.chainLen
			numItems := pg.(*page).head.numItems
			memUsed := pg.ComputeMemUsed()
			if n.Item() == skiplist.MinItem {
				w.WriteString(fmt.Sprintf("[%d] \"\" deltaLen:%d numItems:%d memused:%d\n", c, deltaLen, numItems, memUsed))
				return nil
			}

			w.WriteString(fmt.Sprintf("[%d] %s deltaLen:%d numItems:%d memused:%d\n",
				c, hexCodeKey(db, n.Item()), deltaLen, numItems, memUsed))
			return nil
		}

		db.PageVisitor(callb, 1)
	case "rebalance":
		db := getDB()
		wr := getWr(db)

		maxItems := db.Config.MaxPageItems
		minItems := db.Config.MinPageItems
		maxDeltas := db.Config.MaxDeltaChainLen
		maxLSSSegments := db.Config.MaxPageLSSSegments

		if len(args) == 1+4 {
			maxItems = int(args[1].(float64))
			minItems = int(args[2].(float64))
			maxDeltas = int(args[3].(float64))
			maxLSSSegments = int(args[4].(float64))
		}

		callb := func(pid PageId, partn RangePartition) error {
			pg, err := db.ReadPage(pid, nil, false, wr.wCtx)
			if err != nil {
				w.WriteString(fmt.Sprintf("Error: pageRead err %v", err))
				return fmt.Errorf("error")
			}
			db.trySMOs2(pid, pg, wr.wCtx, false, maxItems, minItems, maxDeltas, maxLSSSegments)
			return nil
		}
		db.PageVisitor(callb, 1)

	default:
		w.WriteString(fmt.Sprintf("Invalid command: %s\n", cmd))

	}
	w.WriteString(fmt.Sprintf("-------------------------------------\n"))
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
	w.WriteHeader(200)
	bufw := bufio.NewWriter(w)
	d.Command(req.Cmd, bufw, req.Args...)
	bufw.Flush()
}
