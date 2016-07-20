// Copyright (c) 2016 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

// Package nodetable implements high performance GC optimized Node lookup table
// for Nitro index storage. This table is not thread-safe
//
// Golang map does not need to pay the cost of GC scans if you have native
// fixed size types for both key and value. We use two tables for the node
// lookup table implementation. Fast table and slow table. Fast table stores
// maps crc32(key) to a uint64. Value is a pointer to a skiplist node. Highest
// bit is used to indicate whether there is any hash collision for the crc32
// key used. If the bit is set, that means we need to lookup second table,
// which is the slow table. Slow table has multiple entries which are mapped
// by the same crc32 key.
package nodetable

import "unsafe"
import "fmt"
import "github.com/t3rm1n4l/nitro/skiplist"

var emptyResult ntResult

const approxItemSize = 42

var dbInstances *skiplist.Skiplist

func init() {
	dbInstances = skiplist.New()
}

// EqualKeyFn implements key equality check
type EqualKeyFn func(unsafe.Pointer, []byte) bool

// HashFn implements 32bit hash function on a string
type HashFn func([]byte) uint32

// NodeTable describes lookup table
type NodeTable struct {
	fastHT      map[uint32]uint64
	slowHT      map[uint32][]uint64
	fastHTCount uint64
	slowHTCount uint64
	conflicts   uint64

	hash     HashFn
	keyEqual EqualKeyFn

	res ntResult
}

// CompareNodeTable implements comparator for nodetable instances
func CompareNodeTable(a, b unsafe.Pointer) int {
	return int(uintptr(a)) - int(uintptr(b))
}

const (
	ntNotFound    = 0x00
	ntFoundInFast = 0x01
	ntFoundInSlow = 0x03
	ntFoundMask   = 0x01
)

type ntResult struct {
	status         int
	hash           uint32
	hasConflict    bool
	fastHTHasEntry bool
	fastHTValue    uint64
	slowHTValues   []uint64
	slowHTPos      int
}

// New creates a nodetable instance
func New(hfn HashFn, kfn EqualKeyFn) *NodeTable {
	nt := &NodeTable{
		fastHT:   make(map[uint32]uint64),
		slowHT:   make(map[uint32][]uint64),
		hash:     hfn,
		keyEqual: kfn,
	}

	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)
	dbInstances.Insert(unsafe.Pointer(nt), CompareNodeTable, buf, &dbInstances.Stats)

	return nt
}

// Stats returns nodetable statistics
func (nt *NodeTable) Stats() string {
	return fmt.Sprintf("\nFastHTCount = %d\n"+
		"SlowHTCount = %d\n"+
		"Conflicts   = %d\n"+
		"MemoryInUse = %d\n",
		nt.fastHTCount, nt.slowHTCount, nt.conflicts, nt.MemoryInUse())
}

// MemoryInUse returns memory used by nodetable instance
func (nt *NodeTable) MemoryInUse() int64 {
	return int64(approxItemSize * (nt.fastHTCount + nt.slowHTCount))
}

// Get returns node pointer for the lookup key
func (nt *NodeTable) Get(key []byte) unsafe.Pointer {
	res := nt.find(key)
	if res.status&ntFoundMask == ntFoundMask {
		if res.status == ntFoundInFast {
			return decodePointer(res.fastHTValue)
		}
		return decodePointer(res.slowHTValues[res.slowHTPos])
	}

	return nil
}

// Update inserts or replaces an existing entry
func (nt *NodeTable) Update(key []byte, nptr unsafe.Pointer) (updated bool, oldPtr unsafe.Pointer) {
	res := nt.find(key)
	if res.status&ntFoundMask == ntFoundMask {
		// Found key, replace old pointer value with new one
		updated = true
		if res.status == ntFoundInFast {
			oldPtr = decodePointer(res.fastHTValue)
			nt.fastHT[res.hash] = encodePointer(nptr, res.hasConflict)
		} else {
			oldPtr = decodePointer(res.slowHTValues[res.slowHTPos])
			res.slowHTValues[res.slowHTPos] = encodePointer(nptr, true)
		}
	} else {
		// Insert new key
		updated = false
		newSlowValue := res.fastHTHasEntry && !res.hasConflict
		// Key needs to be inserted into slowHT
		if res.hasConflict || newSlowValue {
			slowHTValues := nt.slowHT[res.hash]
			slowHTValues = append(slowHTValues, encodePointer(nptr, false))
			nt.slowHT[res.hash] = slowHTValues
			// There is an entry already in the fastHT for same crc32 hash
			// We have inserted first entry into the slowHT. Now mark conflict bit.
			if newSlowValue {
				nt.fastHT[res.hash] = encodePointer(decodePointer(nt.fastHT[res.hash]), true)
				nt.conflicts++
			}
			nt.slowHTCount++
		} else {
			// Insert new item into fastHT
			nt.fastHT[res.hash] = encodePointer(nptr, false)
			nt.fastHTCount++
		}
	}

	return
}

// Remove an item from the nodetable
func (nt *NodeTable) Remove(key []byte) (success bool, nptr unsafe.Pointer) {
	res := nt.find(key)
	if res.status&ntFoundMask == ntFoundMask {
		success = true
		if res.status == ntFoundInFast {
			nptr = decodePointer(res.fastHTValue)
			// Key needs to be removed from fastHT. For that we need to move
			// an item present in slowHT and overwrite fastHT entry.
			if res.hasConflict {
				slowHTValues := nt.slowHT[res.hash]
				v := slowHTValues[0] // New fastHT candidate
				slowHTValues = append([]uint64(nil), slowHTValues[1:]...)
				nt.slowHTCount--

				var conflict bool
				if len(slowHTValues) == 0 {
					delete(nt.slowHT, res.hash)
					nt.conflicts--
				} else {
					conflict = true
					nt.slowHT[res.hash] = slowHTValues
				}

				nt.fastHT[res.hash] = encodePointer(decodePointer(v), conflict)
			} else {
				delete(nt.fastHT, res.hash)
				nt.fastHTCount--
			}
		} else {
			nptr = decodePointer(res.slowHTValues[res.slowHTPos])
			// Remove key from slowHT
			newSlowValue := append([]uint64(nil), res.slowHTValues[:res.slowHTPos]...)
			if res.slowHTPos+1 != len(res.slowHTValues) {
				newSlowValue = append(newSlowValue, res.slowHTValues[res.slowHTPos+1:]...)
			}
			nt.slowHTCount--

			if len(newSlowValue) == 0 {
				delete(nt.slowHT, res.hash)
				nt.fastHT[res.hash] = encodePointer(decodePointer(nt.fastHT[res.hash]), false)
				nt.conflicts--
			} else {
				nt.slowHT[res.hash] = newSlowValue
			}
		}
	}
	return
}

func decodePointer(v uint64) unsafe.Pointer {
	var x uintptr
	if unsafe.Sizeof(x) == 8 {
		ptr := uintptr(v & ^(uint64(1) << 63))
		return unsafe.Pointer(ptr)
	}
	return unsafe.Pointer(uintptr(v & 0xffffffff))
}

func encodePointer(p unsafe.Pointer, hasConflict bool) uint64 {
	v := uint64(uintptr(p))
	if hasConflict {
		v |= 1 << 63
	}

	return v
}

func (nt *NodeTable) hasConflict(v uint64) bool {
	return v>>63 == 1
}

func (nt *NodeTable) isEqual(key []byte, v uint64) bool {
	p := decodePointer(v)
	return nt.keyEqual(p, key)
}

func (nt *NodeTable) find(key []byte) (res *ntResult) {
	nt.res = emptyResult
	res = &nt.res
	res.status = ntNotFound
	h := nt.hash(key)
	res.hash = h

	v, ok := nt.fastHT[h]
	res.fastHTHasEntry = ok
	if ok {
		res.hasConflict = nt.hasConflict(v)
		if nt.isEqual(key, v) {
			res.status = ntFoundInFast
			res.fastHTValue = v
			return
		}

		if res.hasConflict {
			if vs, ok := nt.slowHT[h]; ok {
				for i, v := range vs {
					if nt.isEqual(key, v) {
						res.slowHTPos = i
						res.slowHTValues = vs
						res.status = ntFoundInSlow
						return
					}
				}
			}
		}
	}

	return
}

// Close destroys the nodetable
func (nt *NodeTable) Close() {
	nt.fastHTCount = 0
	nt.slowHTCount = 0
	nt.conflicts = 0
	nt.fastHT = make(map[uint32]uint64)
	nt.slowHT = make(map[uint32][]uint64)

	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)
	dbInstances.Delete(unsafe.Pointer(nt), CompareNodeTable, buf, &dbInstances.Stats)
}

// MemoryInUse returns total memory used by nodetables in a process
func MemoryInUse() (sz int64) {
	buf := dbInstances.MakeBuf()
	defer dbInstances.FreeBuf(buf)
	iter := dbInstances.NewIterator(CompareNodeTable, buf)
	for iter.SeekFirst(); iter.Valid(); iter.Next() {
		db := (*NodeTable)(iter.Get())
		sz += db.MemoryInUse()
	}

	return
}
