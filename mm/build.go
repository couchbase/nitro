// +build jemalloc

package mm

// #cgo CFLAGS: -DJEMALLOC=1
// #cgo LDFLAGS: -ljemalloc
import "C"
