// Copyright (c) 2017 Couchbase, Inc.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//   http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the
// License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// either express or implied. See the License for the specific language governing permissions
// and limitations under the License.

package plasma

//#cgo LDFLAGS: -lsigar
//#include <sigar.h>
import "C"
import (
	"fmt"
	"github.com/couchbase/nitro/mm"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	MTunerMaxFreeMemory   = 6 * 1024 * 1024 * 1024
	MTunerMinFreeMemRatio = 0.10
	MTunerTrimDownRatio   = 0.10
	MTunerIncrementRatio  = 0.3
	MTunerMinQuotaRatio   = 0.5
	MTunerIncrCeilPercent = float64(3)
)

var mtunerMutex sync.Mutex
var sigarHandle *C.sigar_t
var isTunerActive bool

type SystemStats struct {
	TotalMemory int64
	FreeMemory  int64
	FreePercent float64
}

func init() {
	C.sigar_open(&sigarHandle)
}

func GetSystemStats() (SystemStats, error) {
	var stats SystemStats
	var v C.sigar_mem_t
	if C.sigar_mem_get(sigarHandle, &v) != C.SIGAR_OK {
		return stats, fmt.Errorf("Unable to fetch system memory")
	}

	stats.FreePercent = float64(v.free_percent)
	stats.FreeMemory = int64(v.actual_free)
	stats.TotalMemory = int64(v.total)
	return stats, nil
}

func RunMemQuotaTuner() {
	func() {
		mtunerMutex.Lock()
		defer mtunerMutex.Unlock()

		if isTunerActive {
			return
		}

		isTunerActive = true
	}()

	for {
		sys, err := GetSystemStats()
		if err != nil {
			logger.Errorf("Plasma: Unable to initialize memory tuner %v", err)
			return
		}

		if float64(sys.TotalMemory)*MTunerMinFreeMemRatio > float64(MTunerMaxFreeMemory) {
			MTunerMinFreeMemRatio = float64(MTunerMaxFreeMemory) / float64(sys.TotalMemory)
		}

		activeQuota := atomic.LoadInt64(&activeMemQuota)
		minQuota := int64(float64(activeQuota) * MTunerMinQuotaRatio)
		currQuota := atomic.LoadInt64(&memQuota)
		incrSize := MTunerIncrementRatio * float64(activeMemQuota)
		inuse := MemoryInUse()

		availFree := sys.FreeMemory
		if inuse < currQuota {
			availFree -= currQuota - inuse
		}

		availFreePercent := float64(availFree)*100/float64(sys.TotalMemory) - MTunerIncrCeilPercent

		if float64(sys.FreePercent)/100 < MTunerMinFreeMemRatio {
			if currQuota > minQuota {
				newQuota := int64(float64(inuse) * MTunerTrimDownRatio)
				logger.Infof("Plasma: Adaptive memory quota tuning (decrementing): minFreePercent:%v, freePercent:%v, currentQuota=%d, newQuota=%d", MTunerMinFreeMemRatio*100, sys.FreePercent, currQuota, newQuota)
				atomic.StoreInt64(&memQuota, newQuota)

				mm.FreeOSMemory()
				runtime.GC()
				time.Sleep(time.Second * 5)
				mm.FreeOSMemory()
				runtime.GC()
			}
		} else if availFreePercent/100 > MTunerMinFreeMemRatio {
			newQuota := int64(float64(currQuota) + incrSize)
			if newQuota < activeQuota {
				atomic.StoreInt64(&memQuota, newQuota)
				logger.Infof("Plasma: Adaptive memory quota tuning (incrementing): minFreePercent:%v, freePercent: %v, currentQuota=%d, newQuota=%d", MTunerMinFreeMemRatio*100, sys.FreePercent, currQuota, newQuota)
			}
		}

		time.Sleep(time.Second * 5)
	}
}
