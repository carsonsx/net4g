package net4g

import (
	"fmt"
	"github.com/carsonsx/log4g"
	"reflect"
	"runtime/debug"
	"sync"
	"time"
)

func Dispatch(dispatchers []*Dispatcher, agent NetAgent) {
	found := false
	for _, p := range dispatchers {
		if p.running {
			p.dispatchChan <- agent
			found = true
		}
	}
	if !found {
		log4g.Warn("not found any running Dispatcher")
	}
}

func NeedDeserialize(dispatchers []*Dispatcher, t reflect.Type) bool {
	for _, p := range dispatchers {
		if p.NeedDeserialize(t) {
			return true
		}
	}
	return false
}

func NewDispatcher(goroutineNum ...int) *Dispatcher {
	return NewNamedDispatcher(NetConfig.ServerName, goroutineNum...)
}

func NewNamedDispatcher(name string, goroutineNum ...int) *Dispatcher {
	p := new(Dispatcher)
	p.Name = name

	p.connCreatedChan = make(chan NetAgent, 100)
	p.connClosedChan = make(chan NetAgent, 100)
	p.dispatchChan = make(chan NetAgent, 1000)
	p.timerEventChan = make(chan *NetTimerEvent, 10000)

	p.destroyChan = make(chan bool, 1)
	p.msgHandlers = make(map[interface{}]func(agent NetAgent))
	p.rawHandlers = make(map[interface{}]func(agent NetAgent))
	if len(goroutineNum) > 0 {
		p.goroutineNum = goroutineNum[0]
	} else {
		p.goroutineNum = 1
	}
	p.listen()
	log4g.Info("new %d %s dispatcher", p.goroutineNum, name)
	return p
}

type Dispatcher struct {
	Name              string
	serializer        Serializer
	globalMsgHandlers []func(agent NetAgent)
	globalRawHandlers []func(agent NetAgent)
	msgHandlers       map[interface{}]func(agent NetAgent)
	rawHandlers       map[interface{}]func(agent NetAgent)
	//before_interceptors       []func(agent NetAgent)
	//after_interceptors        []func(agent NetAgent)
	needDeserialize           sync.Map
	connCreatedChan           chan NetAgent
	connClosedChan            chan NetAgent
	dispatchChan              chan NetAgent
	timerEventChan            chan *NetTimerEvent
	destroyChan               chan bool
	connectionCreatedHandlers []func(agent NetAgent)
	connectionClosedHandlers  []func(agent NetAgent)
	destroyHandler            func()
	goroutineNum              int
	running                   bool
	wg                        sync.WaitGroup
}

func (p *Dispatcher) AddHandler(h func(agent NetAgent), idOrType ...interface{}) {
	if len(idOrType) > 0 {
		id := idOrType[0]
		if reflect.TypeOf(id).Kind() == reflect.Ptr {
			id = reflect.TypeOf(id)
		}
		p.msgHandlers[id] = h
		log4g.Info("dispatcher[%s] added a handler for id[%v]", p.Name, id)
	} else {
		p.globalMsgHandlers = append(p.globalMsgHandlers, h)
		log4g.Info("dispatcher[%s] added global handler", p.Name)
	}
}

func (p *Dispatcher) AddRawHandler(h func(agent NetAgent), idOrType ...interface{}) {
	if len(idOrType) > 0 {
		id := idOrType[0]
		if reflect.TypeOf(id).Kind() == reflect.Ptr {
			id = reflect.TypeOf(id)
		}
		p.rawHandlers[id] = h
		log4g.Info("dispatcher[%s] added a raw handler for id[%v]", p.Name, id)
	} else {
		p.globalRawHandlers = append(p.globalMsgHandlers, h)
		log4g.Info("dispatcher[%s] added global raw handler", p.Name)
	}
}

func (p *Dispatcher) NeedDeserialize(t reflect.Type) bool {
	if t == nil {
		log4g.Warn("nil deserialize type")
		return true
	}
	var need bool
	var v interface{}
	var ok bool
	if v, ok = p.needDeserialize.Load(t); !ok {
		_, hasMagHandler := p.msgHandlers[t]
		hasGlobalMsgHandler := len(p.globalMsgHandlers) > 0
		need = hasMagHandler || hasGlobalMsgHandler
		p.needDeserialize.Store(t, need)
	} else {
		need = v.(bool)
	}
	return need
}

func (p *Dispatcher) OnConnectionCreated(h func(agent NetAgent)) {
	p.connectionCreatedHandlers = append(p.connectionCreatedHandlers, h)
}

func (p *Dispatcher) OnConnectionClosed(h func(agent NetAgent)) {
	p.connectionClosedHandlers = append(p.connectionClosedHandlers, h)
}

func (p *Dispatcher) NewTimer(d time.Duration, h func(t *time.Time, params ...interface{}), params ...interface{}) NetTimer {
	return NewNetTimer(p, d, h, params...)
}

func (p *Dispatcher) NewTicker(d time.Duration, h func(t *time.Time, params ...interface{}), params ...interface{}) NetTimer {
	return NewNetTicker(p, d, h, params...)
}

func (p *Dispatcher) OnDestroy(h func()) {
	p.destroyHandler = h
}

func (p *Dispatcher) listen() {
	//log4g.Debug("Dispatcher goroutine number: %d", p.goroutineNum)
	p.wg.Add(p.goroutineNum)
	counter := 1
	for i := 0; i < p.goroutineNum; i++ {
		go func() {
			defer p.wg.Done()
			th := fmt.Sprintf("%dth", counter)
			if counter == 1 {
				th = "1st"
			} else if counter == 2 {
				th = "2nd"
			}
			log4g.Info("started %s goroutine of dispatcher[%s]", th, p.Name)
			counter++
		outer:
			for {
				select {
				case <-p.destroyChan:
					p.onDestroyHandler()
					break outer
				case agent := <-p.connClosedChan:
					p.onConnectionClosedHandlers(agent)
				case agent := <-p.connCreatedChan:
					p.onConnectionCreatedHandlers(agent)
				case te := <-p.timerEventChan:
					if te.handler != nil {
						te.handler(te.time, te.params...)
					}
				case agent := <-p.dispatchChan:
					p.dispatch(agent)
				}
			}
		}()
	}

	p.running = true
}

func (p *Dispatcher) dispatch(agent NetAgent) {

	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Message Handler Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			agent.Close()
			log4g.Error("********************* Message Handler Panic *********************")
		}
	}()

	//for _, i := range p.before_interceptors {
	//	i(agent)
	//}

	for _, h := range p.globalMsgHandlers {
		h(agent)
	}
	var msgId interface{}
	//var rawId interface{}
	var msgHandler func(agent NetAgent)
	var rawHandler func(agent NetAgent)
	rp := agent.RawPack()
	if rp != nil {
		if rp.Type != nil {
			msgId = rp.Type
			//rawId = rp.Type
			msgHandler, _ = p.msgHandlers[rp.Type]
			rawHandler, _ = p.rawHandlers[rp.Type]
		}
		if msgHandler == nil {
			if rp.Id != nil {
				msgId = rp.Id
				msgHandler, _ = p.msgHandlers[rp.Id]
			}
		}
		if rawHandler == nil {
			if rp.Id != nil {
				//rawId = rp.Id
				msgHandler, _ = p.msgHandlers[rp.Id]
			}
		}
	}
	if msgHandler == nil && agent.Msg() != nil {
		msgId = reflect.TypeOf(agent.Msg())
		msgHandler = p.msgHandlers[msgId]
	}

	if rawHandler != nil {
		log4g.Trace("%v is dispatching to %s", msgId, p.Name)
		rawHandler(agent)
	}

	if msgHandler != nil {
		log4g.Trace("%v is dispatching to %s", msgId, p.Name)
		msgHandler(agent)
	}

	//for _, i := range p.after_interceptors {
	//	i(agent)
	//}
}

func (p *Dispatcher) onConnectionCreatedHandlers(agent NetAgent) {

	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Created Handler Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			log4g.Error("********************* Created Handler Panic *********************")
		}
	}()

	for _, h := range p.connectionCreatedHandlers {
		h(agent)
	}
}

func (p *Dispatcher) onConnectionClosedHandlers(agent NetAgent) {

	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Closed Handler Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			log4g.Error("********************* Closed Handler Panic *********************")
		}
	}()

	for _, h := range p.connectionClosedHandlers {
		h(agent)
	}
}

func (p *Dispatcher) onDestroyHandler() {

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

func (p *Dispatcher) dispatchConnectionCreatedEvent(agent NetAgent) {
	p.connCreatedChan <- agent
	log4g.Debug("dispatched connection created event")
}

func (p *Dispatcher) dispatchConnectionClosedEvent(agent NetAgent) {
	p.connClosedChan <- agent
	log4g.Debug("dispatched connection closed event")
}

func (p *Dispatcher) Destroy() {
	if p.running {
		for i := 0; i < p.goroutineNum; i++ {
			p.destroyChan <- true
		}
	}
	p.running = false
	p.wg.Wait()
	//how to close gracefully
	//close(p.dispatchChan)
	//close(p.sessionClosedChan)
	//close(p.destroyChan)
}
