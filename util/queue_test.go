package util

import (
	"testing"
)

func TestQueue(t *testing.T) {
	q := NewSafeQueue()
	q.Offer("a")
	q.Offer("b")
	q.Offer("c")

	if q.Poll() != "a" {
		t.FailNow()
	}

	if q.Peek() != "b" {
		t.FailNow()
	}

	if q.Peek() != "b" {
		t.FailNow()
	}

	q.Offer("d")

	if q.Poll() != "b" {
		t.FailNow()
	}

	if q.Poll() != "c" {
		t.FailNow()
	}

	if q.Poll() != "d" {
		t.FailNow()
	}

	if q.Len() != 0 {
		t.FailNow()
	}

}
