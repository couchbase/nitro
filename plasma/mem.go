package plasma

import (
	"fmt"
	"os"
	"runtime"
	"syscall"
)

var linuxMemFd *os.File

func init() {
	linuxMemFd, _ = os.Open("/proc/self/statm")
}

func ProcessRSS() (rss int) {
	os := runtime.GOOS
	if os == "darwin" {
		var result syscall.Rusage
		syscall.Getrusage(syscall.RUSAGE_SELF, &result)
		return int(result.Maxrss)
	} else if os == "linux" {
		var x int
		var memReadBuf [256]byte
		n, _ := linuxMemFd.ReadAt(memReadBuf[:], 0)
		fmt.Sscan(string(memReadBuf[:n]), &x, &rss)
		return rss * 4096
	}

	panic(fmt.Sprintf("GOOS=%v unsupported", os))
	return
}
