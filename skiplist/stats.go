package skiplist

import "fmt"

type StatsReport struct {
	ReadConflicts       uint64
	InsertConflicts     uint64
	NextPointersPerNode float64
	NodeDistribution    [MaxLevel + 1]int64
	NodeCount           int
	SoftDeletes         int64
}

type stats struct {
	insertConflicts uint64
	readConflicts   uint64
	levelNodesCount [MaxLevel + 1]int64
	softDeletes     int64
}

func (s StatsReport) String() string {
	str := "\nskiplist stats\n==============\n"
	str += fmt.Sprintf(
		"node_count             = %d\n"+
			"soft_deletes           = %d\n"+
			"read_conflicts         = %d\n"+
			"insert_conflicts       = %d\n"+
			"next_pointers_per_node = %.4f\n\n",
		s.NodeCount, s.SoftDeletes, s.ReadConflicts, s.InsertConflicts,
		s.NextPointersPerNode)

	str += "level_node_distribution:\n"

	for i, c := range s.NodeDistribution {
		str += fmt.Sprintf("level%d => %d\n", i, c)
	}

	return str
}

func (s *Skiplist) GetStats() StatsReport {
	var report StatsReport
	var totalNextPtrs int
	var totalNodes int
	report.ReadConflicts = s.stats.readConflicts
	report.InsertConflicts = s.stats.insertConflicts

	for i, c := range s.stats.levelNodesCount {
		totalNodes += int(c)
		totalNextPtrs += (i + 1) * int(c)
	}

	report.SoftDeletes = s.stats.softDeletes
	report.NodeCount = totalNodes
	report.NodeDistribution = s.stats.levelNodesCount
	report.NextPointersPerNode = float64(totalNextPtrs) / float64(totalNodes)
	return report
}
