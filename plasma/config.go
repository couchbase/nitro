package plasma

import (
	"github.com/t3rm1n4l/nitro/skiplist"
	"runtime"
	"unsafe"
)

type Config struct {
	MaxDeltaChainLen int
	MaxPageItems     int
	MinPageItems     int
	Compare          skiplist.CompareFn
	ItemSize         ItemSizeFn

	LSSLogSegmentSize   int64
	File                string
	FlushBufferSize     int
	NumPersistorThreads int
	NumEvictorThreads   int

	LSSCleanerThreshold int
	AutoLSSCleaning     bool
	AutoSwapper         bool

	EnableShapshots bool

	// TODO: Remove later
	MaxMemoryUsage int

	ContinueSwapper func() bool
	TriggerSwapper  func() bool
	shouldPersist   bool

	MaxSnSyncFrequency int
	SyncInterval       int
}

func applyConfigDefaults(cfg Config) Config {
	if cfg.NumPersistorThreads == 0 {
		cfg.NumPersistorThreads = runtime.NumCPU()
	}

	if cfg.NumEvictorThreads == 0 {
		cfg.NumEvictorThreads = runtime.NumCPU()
	}

	// TODO: Remove later
	if cfg.TriggerSwapper == nil && cfg.ContinueSwapper == nil && cfg.MaxMemoryUsage > 0 {
		swapper := func() bool {
			return ProcessRSS() >= int(0.7*float32(cfg.MaxMemoryUsage))
		}

		cfg.TriggerSwapper = swapper
		cfg.ContinueSwapper = swapper
	}

	if cfg.File == "" {
		cfg.AutoLSSCleaning = false
		cfg.AutoSwapper = false
	} else {
		cfg.shouldPersist = true
	}

	if cfg.MaxSnSyncFrequency == 0 {
		cfg.MaxSnSyncFrequency = 360000
	}

	if cfg.LSSLogSegmentSize == 0 {
		cfg.LSSLogSegmentSize = 1024 * 1024 * 1024 * 4
	}

	return cfg
}

func DefaultConfig() Config {
	return Config{
		MaxDeltaChainLen: 200,
		MaxPageItems:     400,
		MinPageItems:     25,
		Compare:          cmpItem,
		ItemSize: func(itm unsafe.Pointer) uintptr {
			if itm == skiplist.MinItem || itm == skiplist.MaxItem {
				return 0
			}
			return uintptr((*item)(itm).Size())
		},
		FlushBufferSize:     1024 * 1024,
		LSSCleanerThreshold: 10,
		AutoLSSCleaning:     true,
		AutoSwapper:         false,
		EnableShapshots:     true,
		MaxMemoryUsage:      1024 * 1024 * 1024 * 512,
		SyncInterval:        0,
	}
}
