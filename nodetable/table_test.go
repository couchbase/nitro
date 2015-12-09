package nodetable

import "testing"
import "bytes"
import "hash/crc32"
import "unsafe"
import "fmt"
import "time"
import "syscall"
import "runtime/debug"

type object struct {
	key   []byte
	value int
}

func equalObject(p unsafe.Pointer, k []byte) bool {
	obj := (*object)(p)
	return bytes.Equal(obj.key, k)
}

func mkHashFun(h uint32) HashFn {
	return func([]byte) uint32 {
		return h
	}
}

func dumpTable(tab *NodeTable) {
	fmt.Println("==NodeTable==")
	count := 0
	for k, v := range tab.fastHT {
		o := (*object)(decodePointer(v))
		fmt.Printf("hash:%d, keys:%s,", k, string(o.key))
		count++
		if vs, ok := tab.slowHT[k]; ok {
			for _, v := range vs {
				o := (*object)(decodePointer(v))
				fmt.Printf("%s,", string(o.key))
				count++
			}
		}
		fmt.Println("")
	}

	fmt.Println("Total:", count)
}

func mkObject(key string, v int) *object {
	return &object{
		key:   []byte(key),
		value: v,
	}
}

func TestPointerEncode(t *testing.T) {
	o1 := unsafe.Pointer(mkObject("key", 1000))
	v := encodePointer(o1, true)
	o2 := decodePointer(v)

	if o1 != o2 {
		t.Errorf("Expected encoded value to remain the same with conflict %p!=%p", o1, o2)
	}

	v = encodePointer(o1, false)
	o2 = decodePointer(v)

	if o1 != o2 {
		t.Errorf("Expected encoded value to remain the same without conflict %p!=%p", o1, o2)
	}
}

func TestInsertFastHT(t *testing.T) {
	table := New(mkHashFun(100), equalObject)
	o1 := mkObject("key", 1000)
	table.Update(o1.key, unsafe.Pointer(o1))
	o2 := (*object)(table.Get(o1.key))
	if o2 != o1 {
		t.Errorf("Expected same object")
	}
}

func TestInsertSlowHT(t *testing.T) {
	table := New(mkHashFun(100), equalObject)
	o1 := mkObject("key1", 1000)
	o2 := mkObject("key2", 2000)
	o3 := mkObject("key3", 3000)
	table.Update(o1.key, unsafe.Pointer(o1))
	table.Update(o2.key, unsafe.Pointer(o2))
	table.Update(o3.key, unsafe.Pointer(o3))
	ro1 := (*object)(table.Get(o1.key))
	ro2 := (*object)(table.Get(o2.key))
	ro3 := (*object)(table.Get(o3.key))
	if o1 != ro1 || o2 != ro2 || o3 != ro3 {
		t.Errorf("Expected same objects %p!=%p, %p!=%p, %p!=%p", o1, ro1, o2, ro2, o3, ro3)
	}
}

func TestUpdateFastHT(t *testing.T) {
	table := New(mkHashFun(100), equalObject)
	o1 := mkObject("key", 1000)
	o2 := mkObject("key", 2000)
	updated, old := table.Update(o1.key, unsafe.Pointer(o1))
	if updated != false || old != nil {
		t.Errorf("Expected successful insert")
	}

	updated, old = table.Update(o2.key, unsafe.Pointer(o2))
	if updated != true || (*object)(old) != o1 {
		t.Errorf("Expected old object to be returned")
	}
}

func TestUpdateSlowHT(t *testing.T) {
	table := New(mkHashFun(100), equalObject)
	o1 := mkObject("key0", 1000)
	o2 := mkObject("key1", 2000)
	o3 := mkObject("key1", 6000)
	updated, old := table.Update(o1.key, unsafe.Pointer(o1))
	if updated != false || old != nil {
		t.Errorf("Expected successful insert")
	}

	table.Update(o2.key, unsafe.Pointer(o2))
	updated, old = table.Update(o3.key, unsafe.Pointer(o3))
	if updated != true || (*object)(old) != o2 {
		t.Errorf("Expected old object to be returned")
	}
}

func TestDeleteFastHT1(t *testing.T) {
	table := New(mkHashFun(100), equalObject)
	o1 := mkObject("key", 1000)
	table.Update(o1.key, unsafe.Pointer(o1))
	o2 := (*object)(table.Get(o1.key))
	if o2 != o1 {
		t.Errorf("Expected same object")
	}

	if success, _ := table.Remove(o1.key); success != true {
		t.Errorf("Expected successful remove")
	}

	o3 := (*object)(table.Get(o1.key))
	if o3 != nil {
		t.Errorf("Expected not-found")
	}

	if success, _ := table.Remove(o1.key); success == true {
		t.Errorf("Expected remove fail")
	}
}

func TestDeleteFastHT2(t *testing.T) {
	table := New(mkHashFun(100), equalObject)
	o1 := mkObject("key1", 1000)
	o2 := mkObject("key2", 2000)
	o3 := mkObject("key3", 3000)
	table.Update(o1.key, unsafe.Pointer(o1))
	table.Update(o2.key, unsafe.Pointer(o2))
	table.Update(o3.key, unsafe.Pointer(o3))

	if success, _ := table.Remove(o1.key); success != true {
		t.Errorf("Expected successful remove")
	}

	ro1 := (*object)(table.Get(o1.key))
	ro2 := (*object)(table.Get(o2.key))
	ro3 := (*object)(table.Get(o3.key))

	if ro1 != nil {
		t.Errorf("Expected not found")
	}

	if ro2 != o2 || ro3 != o3 {
		t.Errorf("Expected to find those objects")
	}
}

func TestDeleteSlowHT1(t *testing.T) {
	table := New(mkHashFun(100), equalObject)
	o1 := mkObject("key1", 1000)
	o2 := mkObject("key2", 2000)
	o3 := mkObject("key3", 3000)
	table.Update(o1.key, unsafe.Pointer(o1))
	table.Update(o2.key, unsafe.Pointer(o2))
	table.Update(o3.key, unsafe.Pointer(o3))

	if success, _ := table.Remove(o2.key); success != true {
		t.Errorf("Expected successful remove")
	}

	ro1 := (*object)(table.Get(o1.key))
	ro2 := (*object)(table.Get(o2.key))
	ro3 := (*object)(table.Get(o3.key))

	if ro2 == nil {
		t.Errorf("Expected not found")
	}

	if ro1 != o1 || ro3 != o3 {
		t.Errorf("Expected to find those objects")
	}
}

func TestDeleteFastHT3(t *testing.T) {
	table := New(mkHashFun(100), equalObject)
	o1 := mkObject("key1", 1000)
	o2 := mkObject("key2", 2000)
	table.Update(o1.key, unsafe.Pointer(o1))
	table.Update(o2.key, unsafe.Pointer(o2))

	res := table.find(o1.key)
	if !table.hasConflict(res.fastHTValue) {
		t.Errorf("Expected conflict")
	}

	if success, _ := table.Remove(o2.key); success != true {
		t.Errorf("Expected successful remove")
	}

	ro1 := (*object)(table.Get(o1.key))
	ro2 := (*object)(table.Get(o2.key))

	if ro2 != nil {
		t.Errorf("Expected not found")
	}

	if ro1 != o1 {
		t.Errorf("Expected found")
	}

	res = table.find(o1.key)
	if table.hasConflict(res.fastHTValue) {
		t.Errorf("Expected no conflict")
	}

}

func TestSimple(t *testing.T) {
	table := New(crc32.ChecksumIEEE, equalObject)
	o1 := mkObject("key1", 100)
	o2 := mkObject("key1", 200)
	updated, old := table.Update(o1.key, unsafe.Pointer(o1))
	if updated == true || old != nil {
		t.Errorf("Expected update=false, old=nil")
	}

	updated, old = table.Update(o2.key, unsafe.Pointer(o2))
	if updated == false || old == nil {
	}

	o3 := table.Get(o1.key)
	if o3 == nil {
		t.Errorf("Expected non nil")
	} else {
		o4 := (*object)(o3)
		if o4.value != 200 {
			t.Errorf("Expected value = 200")
		}
	}
}

func TestLargeConflicts(t *testing.T) {
	n := 100000
	hfn := func(k []byte) uint32 {
		return crc32.ChecksumIEEE(k) % 1000
	}
	table := New(hfn, equalObject)
	objects := make([]*object, n)
	for i := 0; i < n; i++ {
		objects[i] = mkObject(fmt.Sprintf("key-%d", i), i)
		updated, _ := table.Update(objects[i].key, unsafe.Pointer(objects[i]))
		if updated {
			t.Errorf("Expected insert")
		}
		ptr := table.Get(objects[i].key)
		if (*object)(ptr) != objects[i] {
			t.Errorf("%s Expected object %p, not %p", objects[i].key, objects[i], ptr)
			dumpTable(table)
		}
	}

	for i := 0; i < n; i++ {
		ptr := table.Get(objects[i].key)
		if (*object)(ptr) != objects[i] {
			t.Errorf("Expected to find the object %s %v", string(objects[i].key), ptr)
			res := table.find(objects[i].key)
			fmt.Println(res)
			fmt.Println(table.Stats())
			dumpTable(table)
			t.Fatalf("failed")
		}
	}

}

func TestMemoryOverhead(t *testing.T) {
	n := 100000
	table := New(crc32.ChecksumIEEE, equalObject)
	objects := make([]*object, n)
	for i := 0; i < n; i++ {
		objects[i] = mkObject(fmt.Sprintf("key-%d", i), i)
	}

	var rusage1, rusage2 syscall.Rusage
	debug.FreeOSMemory()
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage1)
	for i := 0; i < n; i++ {
		table.Update(objects[i].key, unsafe.Pointer(objects[i]))
	}
	debug.FreeOSMemory()
	syscall.Getrusage(syscall.RUSAGE_SELF, &rusage2)

	rss := (rusage2.Maxrss - rusage1.Maxrss)
	fmt.Println("Memory used for hashtable:", rss)
	fmt.Println("Overhead per item:", float32(rss)/float32(n))
}

func TestPerf(t *testing.T) {
	n := 10000000
	table := New(crc32.ChecksumIEEE, equalObject)
	objects := make([]*object, n)
	newobjects := make([]*object, n)
	for i := 0; i < n; i++ {
		objects[i] = mkObject(fmt.Sprintf("key-%d", i), i)
		newobjects[i] = mkObject(fmt.Sprintf("key-%d", i), i+100)
	}

	t0 := time.Now()
	for i := 0; i < n; i++ {
		updated, last := table.Update(objects[i].key, unsafe.Pointer(objects[i]))
		if updated == true || last != nil {
			t.Errorf("Expected updated=false")
		}
	}
	dur := time.Since(t0)
	fmt.Printf("Insert took %v for %v items, %v/s\n", dur, n, float32(n)/float32(dur.Seconds()))

	t0 = time.Now()
	for i := 0; i < n; i++ {
		ptr := table.Get(objects[i].key)
		if ptr == nil {
			t.Fatalf("Expected to find the object")
		}

		o := (*object)(ptr)
		if o != objects[i] {
			t.Errorf("Received unexpected object")
		}
	}
	dur = time.Since(t0)
	fmt.Printf("Get took %v for %v items, %v/s\n", dur, n, float32(n)/float32(dur.Seconds()))

	t0 = time.Now()
	for i := 0; i < n; i++ {
		updated, last := table.Update(objects[i].key, unsafe.Pointer(objects[i]))
		if updated == false || (*object)(last) != objects[i] {
			t.Errorf("Expected updated=true")
		}
	}
	dur = time.Since(t0)
	fmt.Printf("Update took %v for %v items, %v/s\n", dur, n, float32(n)/float32(dur.Seconds()))
	fmt.Println("Table stats:", table.Stats())
}
