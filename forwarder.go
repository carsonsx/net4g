package net4g

import (
	"reflect"
	"github.com/carsonsx/log4g"
	"runtime"
)

func NewForwarder() Forwarder {
	f := new (forwarder)
	f.handlers = make(map[interface{}]func(agent NetAgent) bool)
	return f
}

type Forwarder interface {
	AddHandler(handler func(agent NetAgent) bool, id interface{})
	Handle(id interface{}, agent NetAgent) bool
}

type forwarder struct {
	handlers map[interface{}]func(agent NetAgent) bool
}

func (f *forwarder) AddHandler(handler func(agent NetAgent) bool, id interface{}) {
	if reflect.TypeOf(id).Kind() == reflect.Ptr {
		id = reflect.TypeOf(id)
	}
	f.handlers[id] = handler
	log4g.Info("added a forward handler[%s] for id[%v]",runtime.FuncForPC(reflect.ValueOf(handler).Pointer()).Name(), id)

}

func (f *forwarder) Handle(id interface{}, agent NetAgent) bool {
	if handler, ok := f.handlers[id]; ok {
		return handler(agent)
	}
	return true
}
