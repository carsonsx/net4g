package net4g

import (
	"github.com/carsonsx/log4g"
	"reflect"
	"sync"
)

func Dispatch(dispatchers []*dispatcher, req NetReq, res NetRes) {
	for _, p := range dispatchers {
		p.dispatchChan <- &dispatchData{req: req, res: res}
	}
}

func NewDispatcher(name string) *dispatcher {
	p := new(dispatcher)
	p.Name = name
	p.dispatchChan = make(chan *dispatchData, 1000)
	p.closeSessionChan = make(chan NetSession, 100)
	p.closeChan = make(chan bool, 1)
	p.typeHandlers = make(map[reflect.Type]func(req NetReq, res NetRes))
	p.run()
	log4g.Info("new a %s dispatcher", name)
	return p
}

type dispatchData struct {
	req NetReq
	res NetRes
}

type dispatcher struct {
	Name                string
	serializer          Serializer
	mgr                 *NetManager
	globalHandlers      []func(req NetReq, res NetRes)
	typeHandlers        map[reflect.Type]func(req NetReq, res NetRes)
	before_interceptors []func(req NetReq, res NetRes)
	after_interceptors  []func(req NetReq, res NetRes)
	dispatchChan        chan *dispatchData
	closeSessionChan    chan NetSession
	closeSessionHandler func(session NetSession)
	closeChan           chan bool
	closeHandler        func()
	wg                  sync.WaitGroup
}

func (p *dispatcher) AddHandler(h func(req NetReq, res NetRes), t ...reflect.Type) {
	if len(t) > 0 {
		p.typeHandlers[t[0]] = h
		log4g.Info("added a handler for %v", t[0])
	} else {
		p.globalHandlers = append(p.globalHandlers, h)
		log4g.Info("added global handler")
	}
}

func (p *dispatcher) SetCloseSessionHandler(h func(session NetSession)) {
	p.closeSessionHandler = h
}

func (p *dispatcher) SetCloseHandler(h func()) {
	p.closeHandler = h
}

func (p *dispatcher) dispatch(msg *dispatchData) {
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
		log4g.Trace("not found any handler for %v", t)
	}

	for _, i := range p.after_interceptors {
		i(msg.req, msg.res)
	}
}

func (p *dispatcher) run() {
	p.wg.Add(1)
	// one dispatcher, one goroutine
	go func() {
	outer:
		for {
			select {
			case data := <-p.dispatchChan:
				p.dispatch(data)
			case session := <-p.closeSessionChan:
				if p.closeSessionHandler != nil {
					p.closeSessionHandler(session)
				}
				session.GetValue("wg").(*sync.WaitGroup).Done()
			case <-p.closeChan:
				if p.closeHandler != nil {
					p.closeHandler()
				}
				break outer
			}
		}
		p.wg.Done()
	}()
}

func (p *dispatcher) Broadcast(v interface{}, filter func(session NetSession) bool) error {
	b, err := p.serializer.Serialize(v)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.mgr.Broadcast(b, filter)
	return nil
}

func (p *dispatcher) BroadcastAll(v interface{}) error {
	b, err := p.serializer.Serialize(v)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.mgr.BroadcastAll(b)
	return nil
}

func (p *dispatcher) BroadcastOthers(mySession NetSession, v interface{}) error {
	b, err := p.serializer.Serialize(v)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.mgr.BroadcastOthers(mySession, b)
	return nil
}

func (p *dispatcher) CloseSession(session NetSession) {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	session.SetValue("wg", wg)
	p.closeSessionChan <- session
	wg.Wait()
	session.RemoveValue("wg")
}

func (p *dispatcher) Close() {
	p.closeChan <- true
	p.wg.Wait()
	close(p.dispatchChan)
	close(p.closeSessionChan)
	close(p.closeChan)
}
