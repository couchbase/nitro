package skiplist

import "fmt"

type Stats struct {
	ReadConflicts       int
	InsertConflicts     int
	NextPointersPerNode float64
	NodeDistribution    [MaxLevel + 1]uint32
}

func (s Stats) String() string {
	str := "\nskiplist stats\n==============\n"
	str += fmt.Sprintf(
		"read_conflicts         = %d\n"+
			"insert_conflicts       = %d\n"+
			"next_pointers_per_node = %.4f\n\n",
		s.ReadConflicts, s.InsertConflicts,
		s.NextPointersPerNode)

	str += "level_node_distribution:\n"

	for i, c := range s.NodeDistribution {
		str += fmt.Sprintf("level%d => %d\n", i, c)
	}

	return str
}

func GetStats() Stats {
	var s Stats
	var totalNextPtrs int
	var totalNodes int
	s.ReadConflicts = int(readConflicts)
	s.InsertConflicts = int(insertConflicts)

	for i, c := range levelNodesCount {
		totalNodes += int(c)
		totalNextPtrs += (i + 1) * int(c)
	}
	s.NodeDistribution = levelNodesCount

	s.NextPointersPerNode = float64(totalNextPtrs) / float64(totalNodes)
	return s
}
