package net4g

import (
	"github.com/carsonsx/log4g"
	"github.com/carsonsx/net4g/util"
	"time"
	"os"
	"os/signal"
)

var _functions = util.NewQueue()

func TestCall(functions ...func()) {
	for _, function := range functions {
		_functions.Offer(function)
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
		case <- sig:
	}
	if len(delay) > 0 && delay[0] > 0 {
		time.Sleep(time.Duration(delay[0]) * time.Second)
	}
	closed = true
}

func TestDone(force ...bool) {
	if len(force) > 0 && force[0] {
		done <- true
	} else if _functions.Len() > 0 {
		_functions.Poll().(func())()
	} else {
		log4g.Info("all calls done")
		if !closed {
			done <- true
		}
	}
}
