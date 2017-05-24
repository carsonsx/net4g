package util

import "testing"

func TestMap(t *testing.T) {
	m := NewMap()
	m.Put("a", 1)
	m.Put("b", 2)
	m.Put("c", 3)
	doTestMap(m, t)
	m = NewConcurrentMap()
	m.Put("a", 1)
	m.Put("b", 2)
	m.Put("c", 3)
	doTestMap(m, t)
}

func doTestMap(m Map, t *testing.T)  {
	if v := m.Get("a"); v.(int) != 1 {
		t.Fatalf("expected 1, actual %v", v)
	}
	if v := m.Get("b"); v.(int) != 2 {
		t.Fatalf("expected 2, actual %v", v)
	}
	if v := m.Get("c"); v.(int) != 3 {
		t.Fatalf("expected 3, actual %v", v)
	}
	if m.Size() != 3 {
		t.Fatalf("expected 3, actual %v", m.Size())
	}
	if !m.ContainsKey("a") {
		t.Fatal("expected true, actual false")
	}
	m.Remove("a")
	if m.ContainsKey("a") {
		t.Fatal("expected false, actual true")
	}
	if m.Size() != 2 {
		t.Fatalf("expected 2, actual %v", m.Size())
	}
	m.Clear()
	if !m.IsEmpty() {
		t.Fatal("expected true, actual false")
	}
}