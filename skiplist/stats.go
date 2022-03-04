// Copyright 2016-Present Couchbase, Inc.
//
// Use of this software is governed by the Business Source License included in
// the file licenses/BSL-Couchbase.txt.  As of the Change Date specified in that
// file, in accordance with the Business Source License, use of this software
// will be governed by the Apache License, Version 2.0, included in the file
// licenses/APL2.txt.

package skiplist

import "fmt"
import "sync/atomic"

// StatsReport is used for reporting skiplist statistics
type StatsReport struct {
	ReadConflicts       uint64
	InsertConflicts     uint64
	NextPointersPerNode float64
	NodeDistribution    [MaxLevel + 1]int64
	NodeCount           int
	SoftDeletes         int64
	Memory              int64

	NodeAllocs int64
	NodeFrees  int64
}

// Apply updates the report with provided paritial stats
func (report *StatsReport) Apply(s *Stats) {
	var totalNextPtrs int
	var totalNodes int

	report.ReadConflicts += s.readConflicts
	report.InsertConflicts += s.insertConflicts

	for i, c := range s.levelNodesCount {
		report.NodeDistribution[i] += c
		nodesAtlevel := report.NodeDistribution[i]
		totalNodes += int(nodesAtlevel)
		totalNextPtrs += (i + 1) * int(nodesAtlevel)
	}

	report.SoftDeletes += s.softDeletes
	report.NodeCount = totalNodes
	report.NodeAllocs += s.nodeAllocs
	report.NodeFrees += s.nodeFrees
	report.Memory += s.usedBytes
	if totalNodes != 0 {
		report.NextPointersPerNode = float64(totalNextPtrs) / float64(totalNodes)
	}
}

// Stats keeps stats for a skiplist instance
type Stats struct {
	insertConflicts       uint64
	readConflicts         uint64
	levelNodesCount       [MaxLevel + 1]int64
	softDeletes           int64
	nodeAllocs, nodeFrees int64
	usedBytes             int64

	isLocal bool
}

// IsLocal reports true if the stats is partial
func (s *Stats) IsLocal(flag bool) {
	s.isLocal = flag
}

// AddInt64 provides atomic add
func (s *Stats) AddInt64(src *int64, val int64) {
	if s.isLocal {
		*src += val
	} else {
		atomic.AddInt64(src, val)
	}
}

// AddUint64 provides atomic add
func (s *Stats) AddUint64(src *uint64, val uint64) {
	if s.isLocal {
		*src += val
	} else {
		atomic.AddUint64(src, val)
	}
}

// Merge updates global stats with partial stats and resets partial stats
func (s *Stats) Merge(sts *Stats) {
	atomic.AddUint64(&s.insertConflicts, sts.insertConflicts)
	sts.insertConflicts = 0
	atomic.AddUint64(&s.readConflicts, sts.readConflicts)
	sts.readConflicts = 0
	atomic.AddInt64(&s.softDeletes, sts.softDeletes)
	sts.softDeletes = 0
	atomic.AddInt64(&s.nodeAllocs, sts.nodeAllocs)
	sts.nodeAllocs = 0
	atomic.AddInt64(&s.nodeFrees, sts.nodeFrees)
	sts.nodeFrees = 0
	atomic.AddInt64(&s.usedBytes, sts.usedBytes)
	sts.usedBytes = 0

	for i, val := range sts.levelNodesCount {
		if val != 0 {
			atomic.AddInt64(&s.levelNodesCount[i], val)
			sts.levelNodesCount[i] = 0
		}
	}
}

func (s StatsReport) String() string {
	str := fmt.Sprintf("{\n"+
		`"node_count":             %d,`+"\n"+
		`"soft_deletes":           %d,`+"\n"+
		`"read_conflicts":         %d,`+"\n"+
		`"insert_conflicts":       %d,`+"\n"+
		`"next_pointers_per_node": %.4f,`+"\n"+
		`"memory_used":            %d,`+"\n"+
		`"node_allocs":            %d,`+"\n"+
		`"node_frees":             %d,`+"\n",
		s.NodeCount, s.SoftDeletes, s.ReadConflicts, s.InsertConflicts,
		s.NextPointersPerNode, s.Memory, s.NodeAllocs, s.NodeFrees)

	str += `"level_node_distribution":` + "{\n"

	for i, c := range s.NodeDistribution {
		if i > 0 {
			str += fmt.Sprintf(",\n")
		}
		str += fmt.Sprintf(`"level%d": %d`, i, c)
	}
	str += "\n}\n}"
	return str
}

// GetStats returns skiplist stats
func (s *Skiplist) GetStats() StatsReport {
	var report StatsReport
	report.Apply(&s.Stats)
	return report
}

// MemoryInUse returns memory used by skiplist
func (s *Skiplist) MemoryInUse() int64 {
	return atomic.LoadInt64(&s.Stats.usedBytes)
}
