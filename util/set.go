package util

import "sync"

type Set interface {
	Add(item interface{})
	Remove(item interface{})
	Has(item interface{}) bool
	Range(f func(item interface{}))
	Slice() []interface{}
	Size() int
	IsEmpty() bool
	Clear()
}

func NewSet() Set {
	return newUnsafeSet()
}

func newUnsafeSet() *unsafeSet {
	s := new(unsafeSet)
	s.s = make(map[interface{}]struct{})
	return s
}

type unsafeSet struct {
	s map[interface{}]struct{}
}

func (s *unsafeSet) Add(item interface{}) {
	s.s[item] = struct {}{}
}

func (s *unsafeSet) Remove(item interface{}) {
	delete(s.s, item)
}

func (s *unsafeSet) Has(item interface{}) bool {
	_, ok := s.s[item]
	return ok
}

func (s *unsafeSet) Range(f func(item interface{})) {
	for i := range s.s {
		f(i)
	}
}

func (s *unsafeSet) Slice() []interface{} {
	list := []interface{}{}
	for item := range s.s {
		list = append(list, item)
	}
	return list
}

func (s *unsafeSet) Size() int {
	return len(s.s)
}

func (s *unsafeSet) IsEmpty() bool {
	return s.Size() == 0
}

func (s *unsafeSet) Clear() {
	for k := range s.s {
		delete(s.s, k)
	}
}

func NewSafeSet() Set {
	return newSafeSet(newUnsafeSet())
}

func newSafeSet(unsafeSet *unsafeSet) Set {
	s := new(safeSet)
	s.s = unsafeSet
	return s
}

type safeSet struct {
	s       *unsafeSet
	rwMutex sync.RWMutex
}

func (s *safeSet) Add(item interface{}) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.s.Add(item)
}

func (s *safeSet) Remove(item interface{}) {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.s.Remove(item)
}

func (s *safeSet) Has(key interface{}) bool {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.s.Has(key)
}

func (s *safeSet) Range(f func(item interface{})) {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	s.s.Range(f)
}

func (s *safeSet) Slice() []interface{} {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.s.Slice()
}

func (s *safeSet) Size() int {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.s.Size()
}

func (s *safeSet) IsEmpty() bool {
	s.rwMutex.RLock()
	defer s.rwMutex.RUnlock()
	return s.s.IsEmpty()
}

func (s *safeSet) Clear() {
	s.rwMutex.Lock()
	defer s.rwMutex.Unlock()
	s.s.Clear()
}
