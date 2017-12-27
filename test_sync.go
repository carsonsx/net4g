package net4g

import (
	"github.com/carsonsx/gutil"
	"github.com/carsonsx/log4g"
	"os"
	"os/signal"
	"time"
)

var funcQueue = gutil.NewQueue()

func TestCall(functions ...func()) {
	for _, function := range functions {
		funcQueue.Offer(function)
	}
	TestDone()
}

var done = make(chan bool)
var closed bool

func TestWait(delay ...int) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill)
	select {
	case <-done:
		log4g.Info("[DONE]")
	case <-sig:
	}
	if len(delay) > 0 && delay[0] > 0 {
		time.Sleep(time.Duration(delay[0]) * time.Second)
	}
	closed = true
}

func TestDone(force ...bool) {
	if len(force) > 0 && force[0] {
		done <- true
	} else if funcQueue.Len() > 0 {
		funcQueue.Poll().(func())()
	} else {
		log4g.Info("all calls done")
		if !closed {
			done <- true
		}
	}
}
