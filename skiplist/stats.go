package skiplist

import "fmt"
import "sync/atomic"

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

func (report *StatsReport) Apply(s *Stats) {
	var totalNextPtrs int
	var totalNodes int

	report.ReadConflicts += s.readConflicts
	report.InsertConflicts += s.insertConflicts

	for i, c := range s.levelNodesCount {
		totalNodes += int(c)
		totalNextPtrs += (i + 1) * int(c)
		report.NodeDistribution[i] += s.levelNodesCount[i]
	}

	report.SoftDeletes += s.softDeletes
	report.NodeCount += totalNodes
	report.NextPointersPerNode += float64(totalNextPtrs) / float64(totalNodes)
	report.NodeAllocs += s.nodeAllocs
	report.NodeFrees += s.nodeFrees
	report.Memory += s.usedBytes
}

type Stats struct {
	insertConflicts       uint64
	readConflicts         uint64
	levelNodesCount       [MaxLevel + 1]int64
	softDeletes           int64
	nodeAllocs, nodeFrees int64
	usedBytes             int64

	isLocal bool
}

func (s *Stats) IsLocal(flag bool) {
	s.isLocal = flag
}

func (s *Stats) AddInt64(src *int64, val int64) {
	if s.isLocal {
		*src += val
	} else {
		atomic.AddInt64(src, val)
	}
}

func (s *Stats) AddUint64(src *uint64, val uint64) {
	if s.isLocal {
		*src += val
	} else {
		atomic.AddUint64(src, val)
	}
}

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
	str := fmt.Sprintf(
		"node_count             = %d\n"+
			"soft_deletes           = %d\n"+
			"read_conflicts         = %d\n"+
			"insert_conflicts       = %d\n"+
			"next_pointers_per_node = %.4f\n"+
			"memory_used            = %d\n"+
			"node_allocs            = %d\n"+
			"node_frees             = %d\n\n",
		s.NodeCount, s.SoftDeletes, s.ReadConflicts, s.InsertConflicts,
		s.NextPointersPerNode, s.Memory, s.NodeAllocs, s.NodeFrees)

	str += "level_node_distribution:\n"

	for i, c := range s.NodeDistribution {
		str += fmt.Sprintf("level%d => %d\n", i, c)
	}

	return str
}

func (s *Skiplist) GetStats() StatsReport {
	var report StatsReport
	report.Apply(&s.Stats)
	return report
}

func (s *Skiplist) MemoryInUse() int64 {
	return atomic.LoadInt64(&s.Stats.usedBytes)
}
