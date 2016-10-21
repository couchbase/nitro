package plasma

import (
	"fmt"
	"io/ioutil"
	"runtime"
	"syscall"
)

func ProcessRSS() (rss int) {
	os := runtime.GOOS
	if os == "darwin" {
		var result syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &result)
		return int(result.Maxrss)
	} else if os == "linux" {
		bs, _ := ioutil.ReadFile("/proc/self/statm")
		var x int
		fmt.Sscan(string(bs), &x, &rss)
		return rss * 4096
	}

	panic(fmt.Sprintf("GOOS=%v unsupported", os))
	return
}
