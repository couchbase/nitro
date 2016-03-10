package mm

import (
	"fmt"
	"testing"
)

func TestMalloc(t *testing.T) {
	Malloc(100 * 1024 * 1024)
	fmt.Println("size:", Size())
	fmt.Println(Stats())
}
