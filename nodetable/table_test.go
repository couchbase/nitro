package nodetable

import "testing"
import "bytes"
import "hash/crc32"
import "unsafe"

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

	if table.Remove(o1.key) != true {
		t.Errorf("Expected successful remove")
	}

	o3 := (*object)(table.Get(o1.key))
	if o3 != nil {
		t.Errorf("Expected not-found")
	}

	if table.Remove(o1.key) == true {
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

	if table.Remove(o1.key) != true {
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

	if table.Remove(o2.key) != true {
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

	if table.Remove(o2.key) != true {
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
