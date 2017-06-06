package net4g

import (
	"github.com/carsonsx/log4g"
	"reflect"
	"runtime/debug"
	"sync"
)

func Dispatch(dispatchers []*dispatcher, req NetReq, res NetRes) {
	found := false
	for _, p := range dispatchers {
		if p.running {
			p.dispatchChan <- &dispatchData{req: req, res: res}
			found = true
		}
	}
	if !found {
		log4g.Warn("not found any running dispatcher")
	}
}

func AddHandler(dispatcher *dispatcher, v interface{}, h func(req NetReq, res NetRes)) {
	dispatcher.AddHandler(h, reflect.TypeOf(v))
}

func NewDispatcher(name string) *dispatcher {
	p := new(dispatcher)
	p.Name = name
	p.dispatchChan = make(chan *dispatchData, 1000)
	p.sessionClosedChan = make(chan NetSession, 100)
	p.destroyChan = make(chan bool, 1)
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
	Name                    string
	serializer              Serializer
	mgr                     *NetManager
	globalHandlers          []func(req NetReq, res NetRes)
	typeHandlers            map[reflect.Type]func(req NetReq, res NetRes)
	before_interceptors     []func(req NetReq, res NetRes)
	after_interceptors      []func(req NetReq, res NetRes)
	dispatchChan            chan *dispatchData
	sessionClosedChan       chan NetSession
	connectionClosedHandlers []func(session NetSession)
	destroyChan             chan bool
	destroyHandler          func()
	running                 bool
	wg                      sync.WaitGroup
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

func (p *dispatcher) OnConnectionClosed(h func(session NetSession)) {
	p.connectionClosedHandlers = append(p.connectionClosedHandlers, h)
}

func (p *dispatcher) OnDestroy(h func()) {
	p.destroyHandler = h
}

func (p *dispatcher) dispatch(dData *dispatchData) {

	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Message Handler Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			dData.res.Close()
			log4g.Error("********************* Message Handler Panic *********************")
		}
	}()

	for _, i := range p.before_interceptors {
		i(dData.req, dData.res)
	}

	for _, h := range p.globalHandlers {
		h(dData.req, dData.res)
	}

	t := reflect.TypeOf(dData.req.Msg())
	if h, ok := p.typeHandlers[t]; ok {
		log4g.Trace("dispatcher[%s] is dispatching %v to handler ", p.Name, t)
		h(dData.req, dData.res)
	} else {
		log4g.Trace("dispatcher[%s] not found any handler ", p.Name)
	}

	for _, i := range p.after_interceptors {
		i(dData.req, dData.res)
	}
}

func (p *dispatcher) onConnectionClosedHandlers(session NetSession) {

	defer session.Get("wg").(*sync.WaitGroup).Done()

	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Close Handler Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			log4g.Error("********************* Close Handler Panic *********************")
		}
	}()

	if len(p.connectionClosedHandlers) > 0 {
		for _, h := range p.connectionClosedHandlers {
			h(session)
		}
	}
}


func (p *dispatcher) onDestroyHandler() {

	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Destroy Handler Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			log4g.Error("********************* Destroy Handler Panic *********************")
		}
	}()

	if p.destroyHandler != nil {
		p.destroyHandler()
	}
}

func (p *dispatcher) run() {
	p.wg.Add(1)
	// one dispatcher, one goroutine
	go func() {
		defer p.wg.Done()
	outer:
		for {
			select {
			case <-p.destroyChan:
				p.onDestroyHandler()
				break outer
			case data := <-p.dispatchChan:
				p.dispatch(data)
			case session := <-p.sessionClosedChan:
				p.onConnectionClosedHandlers(session)
			}
		}
	}()

	p.running = true
}

func (p *dispatcher) Kick(filter func(session NetSession) bool) {
	p.mgr.Kick(filter)
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

func (p *dispatcher) Someone(v interface{}, filter func(session NetSession) bool) error {
	b, err := p.serializer.Serialize(v)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.mgr.Someone(b, filter)
	return nil
}

func (p *dispatcher) handleConnectionClosed(session NetSession) {
	log4g.Debug("handling closed session")
	wg := new(sync.WaitGroup)
	wg.Add(1)
	session.Set("wg", wg)
	p.sessionClosedChan <- session
	wg.Wait()
	session.Remove("wg")
	log4g.Debug("handled closed session")
}

func (p *dispatcher) Destroy() {
	if p.running {
		p.destroyChan <- true
	}
	p.running = false
	p.wg.Wait()
	//how to close gracefully
	//close(p.dispatchChan)
	//close(p.sessionClosedChan)
	//close(p.destroyChan)
}
