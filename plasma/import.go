package plasma

import (
	"github.com/t3rm1n4l/nitro/skiplist"
)

type CompareFn skiplist.CompareFn
type IntKeyItem skiplist.IntKeyItem

var (
	maxItem       = skiplist.MaxItem
	CompareInt    = skiplist.CompareInt
	NewIntKeyItem = skiplist.NewIntKeyItem
	IntFromItem   = skiplist.IntFromItem
)
