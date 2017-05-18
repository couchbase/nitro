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
	"github.com/couchbase/nitro/skiplist"
	"sync"
	"unsafe"
)

type PageVisitorCallback func(pid PageId, partn RangePartition) error

type RangePartition struct {
	Shard  int
	MinKey unsafe.Pointer
	MaxKey unsafe.Pointer
}

func (s *Plasma) PageVisitor(callb PageVisitorCallback, concurr int) error {
	var wg sync.WaitGroup
	partitions := s.GetRangePartitions(concurr)
	errors := make([]error, len(partitions))

	for _, partn := range partitions {
		wg.Add(1)
		go func(p RangePartition) {
			defer wg.Done()
			errors[p.Shard] = s.VisitPartition(p, callb)
		}(partn)
	}

	wg.Wait()

	for _, err := range errors {
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Plasma) VisitPartition(partn RangePartition, callb PageVisitorCallback) error {
	buf := s.Skiplist.MakeBuf()
	itr := s.Skiplist.NewIterator(s.cmp, buf)
	itr.SetRefreshInterval(pageVisitorSMRInterval)
	defer itr.Close()

	if partn.MinKey == skiplist.MinItem {
		pid := s.StartPageId()
		if err := callb(pid, partn); err != nil {
			return err
		}
	}

	for itr.Seek(partn.MinKey); itr.Valid() && s.cmp(itr.Get(), partn.MaxKey) < 0; itr.Next() {
		pid := PageId(itr.GetNode())
		if err := callb(pid, partn); err != nil {
			return err
		}
	}

	return nil
}

func (s *Plasma) GetRangePartitions(n int) []RangePartition {
	var partns []RangePartition
	var shard int

	barrier := s.Skiplist.GetAccesBarrier()
	token := barrier.Acquire()
	defer barrier.Release(token)

	partns = append(partns, RangePartition{MinKey: skiplist.MinItem})
	for _, key := range s.Skiplist.GetRangeSplitItems(n) {
		if s.cmp(key, partns[shard].MinKey) > 0 {
			key = s.dup(key)
			partns[shard].MaxKey = key
			shard++
			partns = append(partns, RangePartition{MinKey: key, Shard: shard})
		}
	}

	partns[shard].MaxKey = skiplist.MaxItem
	return partns
}
