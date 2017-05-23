package net4g

import (
	"reflect"
	"log"
)

func Dispatch(dispatchers []*dispatcher, req NetReq, res NetRes)  {
	for _, p := range dispatchers {
		p.dispatchChan <- &dispatchChan{req: req, res: res}
	}
}

func NewDispatcher() *dispatcher {
	p := new(dispatcher)
	p.dispatchChan = make(chan *dispatchChan, 1000)
	p.typeHandlers = make(map[reflect.Type]func(req NetReq, res NetRes))
	p.run()
	log.Println("New a dispatcher")
	return p
}

type dispatchChan struct {
	req NetReq
	res NetRes
}

type dispatcher struct {
	globalHandlers      []func(req NetReq, res NetRes)
	typeHandlers        map[reflect.Type]func(req NetReq, res NetRes)
	before_interceptors []func(req NetReq, res NetRes)
	after_interceptors  []func(req NetReq, res NetRes)
	dispatchChan        chan *dispatchChan
}

func (p *dispatcher) AddHandler(h func(req NetReq, res NetRes), t ...reflect.Type) {
	if len(t) > 0 {
		p.typeHandlers[t[0]] = h
	} else {
		p.globalHandlers = append(p.globalHandlers, h)
	}
}

func (p *dispatcher) run() {

	// one dispatcher, one goroutine
	go func() {
		for {
			msg := <- p.dispatchChan

			for _, i := range p.before_interceptors {
				i(msg.req, msg.res)
			}

			for _, h := range p.globalHandlers {
				h(msg.req, msg.res)
			}

			t := reflect.TypeOf(msg.req.Msg())
			if h, ok := p.typeHandlers[t]; ok {
				h(msg.req, msg.res)
			} else {
				//log.Printf("not found any handler for %v", t)
			}

			for _, i := range p.after_interceptors {
				i(msg.req, msg.res)
			}
		}
	}()

}
