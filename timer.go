package net4g

import (
	"time"
	"github.com/carsonsx/log4g"
)

type NetTimerEvent struct {
	time *time.Time
	params []interface{}
	handler func(time *time.Time, params ...interface{})
}

type NetTimer interface {
	Start(d time.Duration)
	Stop() bool
}

func NewNetTimer(dispatcher *Dispatcher, d time.Duration, h func(time *time.Time, params ...interface{}), params ...interface{}) NetTimer {
	timer := new (netTimer)
	timer.d = dispatcher
	timer.event.params = params
	timer.event.handler = h
	timer.Start(d)
	return timer
}

type netTimer struct {
	rawTimer *time.Timer
	d *Dispatcher
	event NetTimerEvent
	stopChan chan bool
}

func (t *netTimer) Start(d time.Duration) {
	t.rawTimer = time.NewTimer(d)
	t.stopChan = make(chan bool)
	go func() {
		select {
		case time := <- t.rawTimer.C:
			t.event.time = &time
			t.d.timerEventChan <- &t.event
		case <- t.stopChan:
			log4g.Debug("timer stopped")
		}
	}()
}

func (t *netTimer) Stop() bool {
	if t.rawTimer.Stop() {
		t.stopChan <- true
		return true
	}
	return false
}

func NewNetTicker(dispatcher *Dispatcher, d time.Duration, h func(time *time.Time, params ...interface{}), params ...interface{}) NetTimer {
	ticker := new (netTicker)
	ticker.d = dispatcher
	ticker.event.params = params
	ticker.event.handler = h
	ticker.Start(d)
	return ticker
}

type netTicker struct {
	rawTicker *time.Ticker
	d         *Dispatcher
	event     NetTimerEvent
	stopChan  chan bool
}

func (t *netTicker) Start(d time.Duration) {
	t.rawTicker = time.NewTicker(d)
	t.stopChan = make(chan bool)
	go func() {
		for {
			select {
			case time := <- t.rawTicker.C:
				t.event.time = &time
				t.d.timerEventChan <- &t.event
			case <- t.stopChan:
				log4g.Debug("ticker stopped")
			}
		}
	}()
}

func (t *netTicker) Stop() bool {
	t.rawTicker.Stop()
	t.stopChan <- true
	return true
}