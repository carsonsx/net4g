package util

import "sync"

type Map interface {
	Put(key interface{}, value interface{})
	Get(key interface{}, defaultValue ...interface{}) (value interface{})
	Remove(key interface{})
	Pick(key interface{}, defaultValue ...interface{}) (value interface{})
	Has(key interface{}) bool
	Range(f func(key interface{}, value interface{}))
	Size() int
	IsEmpty() bool
	Clear()
}

func NewMap() Map {
	return newUnsafeMap()
}

func newUnsafeMap() *unsafeMap {
	m := new(unsafeMap)
	m.m = make(map[interface{}]interface{})
	return m
}

type unsafeMap struct {
	m map[interface{}]interface{}
}

func (m *unsafeMap) Put(key interface{}, value interface{}) {
	m.m[key] = value
}

func (m *unsafeMap) Get(key interface{}, defaultValue ...interface{}) interface{} {
	value, ok := m.m[key]
	if ok {
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return nil
}

func (m *unsafeMap) Remove(key interface{}) {
	delete(m.m, key)
}

func (m *unsafeMap) Pick(key interface{}, defaultValue ...interface{}) interface{} {
	value, ok := m.m[key]
	if ok {
		delete(m.m, key)
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	}
	return nil
}

func (m *unsafeMap) Has(key interface{}) bool {
	_, ok := m.m[key]
	return ok
}

func (m *unsafeMap) Range(f func(key interface{}, value interface{})) {
	for k, v := range m.m {
		f(k, v)
	}
}

func (m *unsafeMap) Size() int {
	return len(m.m)
}

func (m *unsafeMap) IsEmpty() bool {
	return m.Size() == 0
}

func (m *unsafeMap) Clear() {
	for k := range m.m {
		delete(m.m, k)
	}
}

func NewSafeMap() Map {
	return newSafeMap(newUnsafeMap())
}

func newSafeMap(unsafeMap *unsafeMap) Map {
	m := new(safeMap)
	m.m = unsafeMap
	return m
}

type safeMap struct {
	m       *unsafeMap
	rwMutex sync.RWMutex
}

func (m *safeMap) Put(key interface{}, value interface{}) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	m.m.Put(key, value)
}

func (m *safeMap) Get(key interface{}, defaultValue ...interface{}) interface{} {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	return m.m.Get(key, defaultValue...)
}

func (m *safeMap) Pick(key interface{}, defaultValue ...interface{}) interface{} {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	return m.m.Pick(key, defaultValue...)
}

func (m *safeMap) Remove(key interface{}) {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	m.m.Remove(key)
}

func (m *safeMap) Has(key interface{}) bool {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	return m.m.Has(key)
}

func (m *safeMap) Range(f func(key interface{}, value interface{})) {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	m.m.Range(f)
}

func (m *safeMap) Size() int {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	return m.m.Size()
}

func (m *safeMap) IsEmpty() bool {
	m.rwMutex.RLock()
	defer m.rwMutex.RUnlock()
	return m.m.IsEmpty()
}

func (m *safeMap) Clear() {
	m.rwMutex.Lock()
	defer m.rwMutex.Unlock()
	m.m.Clear()
}
