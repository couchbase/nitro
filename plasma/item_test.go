package plasma

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestCompressItemRun(t *testing.T) {
	buf := newBuffer(0)
	var expected []string
	var itms []unsafe.Pointer
	for i := 0; i < 10; i++ {
		var del bool
		v := []byte(fmt.Sprintf("v-%d", i))
		if i%2 == 0 {
			del = true
			v = nil
		}
		x, _ := newItem([]byte("key"), v, 1000, del, buf)
		b := make([]byte, x.Size())
		itmPtr := unsafe.Pointer(&b[0])
		memcopy(itmPtr, unsafe.Pointer(x), x.Size())
		itms = append(itms, itmPtr)
		expected = append(expected, itemStringer(itms[i]))
	}

	dst := make([]unsafe.Pointer, 10)
	sz := itemRunSize(itms)
	bbuf := make([]byte, sz)
	copyItemRun(itms, dst, unsafe.Pointer(&bbuf[0]))

	for i, itm := range dst {
		if s := itemStringer(itm); s != expected[i] {
			t.Errorf("Expected: '%s', got: '%s'", expected[i], s)
		}
	}
}
