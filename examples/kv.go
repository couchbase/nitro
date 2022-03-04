/*
Copyright 2016-Present Couchbase, Inc.

Use of this software is governed by the Business Source License included in
the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
file, in accordance with the Business Source License, use of this software will
be governed by the Apache License, Version 2.0, included in the file
licenses/APL2.txt.
*/

package main

import (
	"fmt"
	"github.com/couchbase/nitro"
)

func main() {
	cfg := nitro.DefaultConfig()
	cfg.SetKeyComparator(nitro.CompareKV)

	db := nitro.NewWithConfig(cfg)
	defer db.Close()

	w := db.NewWriter()

	w.Put(nitro.KVToBytes([]byte("key1"), []byte("value1")))
	w.Put(nitro.KVToBytes([]byte("key2"), []byte("value2")))
	snap1, _ := db.NewSnapshot()
	w.Delete(nitro.KVToBytes([]byte("key1"), nil))
	w.Put(nitro.KVToBytes([]byte("key1"), []byte("value1-new")))
	snap2, _ := db.NewSnapshot()

	fmt.Println("snapshot 1")
	itr := snap1.NewIterator()
	snap1.Close()
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		k, v := nitro.KVFromBytes(itr.Get())
		fmt.Printf("%s = %s\n", k, v)
	}
	itr.Close()

	fmt.Println("snapshot 2")
	itr = snap2.NewIterator()
	snap2.Close()
	for itr.SeekFirst(); itr.Valid(); itr.Next() {
		k, v := nitro.KVFromBytes(itr.Get())
		fmt.Printf("%s = %s\n", k, v)
	}
	itr.Close()
}
