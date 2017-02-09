# nitro

A high performance in-memory key-value item storage engine written in golang.
The storage engine is based lock-free data structures and scales well with
multicore CPUs.

[![Build Status](https://travis-ci.org/couchbase/nitro.svg?branch=master)](https://travis-ci.org/couchbase/nitro)
[![Go Report Card](https://goreportcard.com/badge/github.com/couchbase/nitro)](https://goreportcard.com/report/github.com/couchbase/nitro)
[![GoDoc](https://godoc.org/github.com/couchbase/nitro?status.svg)](https://godoc.org/github.com/couchbase/nitro)


### Features

- Operations: insert, delete, iterator (lookup, range queries)
- Supports multiple concurrent readers and writers which scales almost linearly
- Database snapshots which facilitates stable and repeatable scans
- Lock-free data structures ensures that concurrent readers and writers doesn't
  block each other
- The memory overhead of metadata of an item is 64 bytes
- Fast snapshotting: Minimal overhead snapshots and they can be created frequently (eg,. every 10ms)
- Optional memory manager based on jemalloc to avoid golang garbage collector
  for higher performance
- Custom key comparator
- Fast backup and restore on disk

### Example usage

    // Create a nitro instance with default config
   	db := nitro.New()
	defer db.Close()

    // Create a writer
    // A writer should be created for every concurrent thread
	w := db.NewWriter()
	for i := 0; i < 100; i++ {
		itm := []byte(fmt.Sprintf("item-%02d", i))
		w.Put(itm)
	}

    // Create an immutable snapshot
	snap1, _ := db.NewSnapshot()

	for i := 0; i < 100; i++ {
		if i%2 == 0 {
			itm := []byte(fmt.Sprintf("item-%02d", i))
			w.Delete(itm)
		}
	}

    // Create an immutable snapshot
	snap2, _ := db.NewSnapshot()

    // Create an iterator for a snapshot
	it1 := snap1.NewIterator()
	count1 := 0
	for it1.SeekFirst(); it1.Valid(); it1.Next() {
		fmt.Println("snap-1", string(it1.Get()))
		count1++
	}

    // Close snapshot and iterator once you have finished using them
	it1.Close()
	snap1.Close()

    // Create an iterator for a snapshot
	it2 := snap2.NewIterator()
	count2 := 0
	for it2.SeekFirst(); it2.Valid(); it2.Next() {
		fmt.Println("snap-2", string(it2.Get()))
		count2++
	}

    // Close snapshot and iterator once you have finished using them
	it2.Close()
	snap2.Close()

	fmt.Println(count2 == count1/2)
    

### License

Apache 2.0
