package util

import (
	"container/list"
	"sync"
)

type Queue interface {
	IsEmpty() bool
	Offers(a []interface{})
	Offer(v interface{})
	Poll() interface{}
	Peek() interface{}
	Clear()
	Len() int
}

type unsafeQueue struct {
	data *list.List
}

func NewQueue() *unsafeQueue {
	q := new(unsafeQueue)
	q.data = list.New()
	return q
}

func (q *unsafeQueue) IsEmpty() bool {
	return q.data == nil || q.data.Len() == 0
}

func (q *unsafeQueue) Offers(a []interface{}) {
	for _, e := range a {
		q.Offer(e)
	}
}

func (q *unsafeQueue) Offer(v interface{}) {
	q.data.PushBack(v)
}

func (q *unsafeQueue) Poll() interface{} {
	if q.IsEmpty() {
		return nil
	}
	v := q.data.Remove(q.data.Front())
	return v
}

func (q *unsafeQueue) Peek() interface{} {
	if q.IsEmpty() {
		return nil
	}
	v := q.data.Front().Value
	return v
}

func (q *unsafeQueue) Clear() {
	q.data = list.New()
}

func (q *unsafeQueue) Len() int {
	if q.data == nil {
		return 0
	}
	return q.data.Len()
}

func NewSafeQueue() *safeQueue {
	q := new(safeQueue)
	q.queue = NewQueue()
	return q
}

type safeQueue struct {
	queue   *unsafeQueue
	rwMutex sync.RWMutex
}

func (q *safeQueue) Offers(a []interface{}) {
	q.rwMutex.Lock()
	defer q.rwMutex.Unlock()
	q.queue.Offers(a)
}

func (q *safeQueue) Offer(v interface{}) {
	q.rwMutex.Lock()
	defer q.rwMutex.Unlock()
	q.queue.Offer(v)
}

func (q *safeQueue) Poll() interface{} {
	q.rwMutex.Lock()
	defer q.rwMutex.Unlock()
	return q.queue.Poll()
}

func (q *safeQueue) Peek() interface{} {
	q.rwMutex.RLock()
	defer q.rwMutex.RUnlock()
	return q.queue.Peek()
}

func (q *safeQueue) Clear() {
	q.rwMutex.Lock()
	defer q.rwMutex.Unlock()
	q.queue.Clear()
}

func (q *safeQueue) Len() int {
	q.rwMutex.RLock()
	defer q.rwMutex.RUnlock()
	return q.queue.Len()
}
