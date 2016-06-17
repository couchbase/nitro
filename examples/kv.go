package main

import (
	"fmt"
	"github.com/t3rm1n4l/nitro"
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
