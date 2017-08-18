package net4g

import (
	"fmt"
	"github.com/carsonsx/log4g"
	"reflect"
	"runtime/debug"
	"sync"
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

	//for _, p := range dispatchers {
	//	if p.running {
	//		select {
	//		case a := <-p.dispatchChan:
	//			log4g.Debug(a)
	//		default:
	//		}
	//
	//	}
	//}
}

func NewDispatcher(name string, goroutineNum ...int) *Dispatcher {
	p := new(Dispatcher)
	p.Name = name
	p.createdChan = make(chan NetAgent, 100)
	p.dispatchChan = make(chan NetAgent, 1000)
	p.sessionClosedChan = make(chan NetAgent, 100)
	p.destroyChan = make(chan bool, 1)
	p.msgHandlers = make(map[interface{}]func(agent NetAgent))
	if len(goroutineNum) > 0 {
		p.goroutineNum = goroutineNum[0]
	} else {
		p.goroutineNum = 1
	}
	p.listen()
	log4g.Info("new a %s Dispatcher", name)
	return p
}

type Dispatcher struct {
	Name                      string
	serializer                Serializer
	hub                       NetHub
	globalHandlers            []func(agent NetAgent)
	msgHandlers               map[interface{}]func(agent NetAgent)
	before_interceptors       []func(agent NetAgent)
	after_interceptors        []func(agent NetAgent)
	createdChan               chan NetAgent
	connectionCreatedHandlers []func(agent NetAgent)
	dispatchChan              chan NetAgent
	sessionClosedChan         chan NetAgent
	connectionClosedHandlers  []func(agent NetAgent)
	destroyChan               chan bool
	destroyHandler            func()
	goroutineNum              int
	running                   bool
	wg                        sync.WaitGroup
}

func (p *Dispatcher) AddHandler(h func(agent NetAgent), id_or_type ...interface{}) {
	if len(id_or_type) > 0 {
		id := id_or_type[0]
		if reflect.TypeOf(id).Kind() == reflect.Ptr {
			id = reflect.TypeOf(id)

		}
		p.msgHandlers[id] = h
		log4g.Info("Dispatcher[%s] added a handler for id[%v]", p.Name, id)
	} else {
		p.globalHandlers = append(p.globalHandlers, h)
		log4g.Info("Dispatcher[%s] added global handler", p.Name)
	}
}

func (p *Dispatcher) OnConnectionCreated(h func(agent NetAgent)) {
	p.connectionCreatedHandlers = append(p.connectionCreatedHandlers, h)
}

func (p *Dispatcher) OnConnectionClosed(h func(agent NetAgent)) {
	p.connectionClosedHandlers = append(p.connectionClosedHandlers, h)
}

func (p *Dispatcher) OnDestroy(h func()) {
	p.destroyHandler = h
}

func (p *Dispatcher) listen() {
	log4g.Info("Dispatcher goroutine number: %d", p.goroutineNum)
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
			log4g.Info("started %s goroutine of Dispatcher[%s]", th, p.Name)
			counter++
		outer:
			for {
				select {
				case <-p.destroyChan:
					p.onDestroyHandler()
					break outer
				case agent := <-p.sessionClosedChan:
					p.onConnectionClosedHandlers(agent)
				case agent := <-p.createdChan:
					p.onConnectionCreatedHandlers(agent)
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

	for _, i := range p.before_interceptors {
		i(agent)
	}

	for _, h := range p.globalHandlers {
		h(agent)
	}

	var id interface{}
	var h func(agent NetAgent)

	if agent.RawPack() != nil && agent.RawPack().Id != nil {
		id = agent.RawPack().Id
		h, _ = p.msgHandlers[id]
	}
	if h == nil && agent.Msg() != nil {
		id = reflect.TypeOf(agent.Msg())
		h = p.msgHandlers[id]
	}

	if h != nil {
		log4g.Trace("Dispatcher[%s] is dispatching %v", p.Name, id)
		h(agent)
	} else {
		log4g.Trace("Dispatcher[%s] not found any handler for %v", p.Name, id)
	}

	for _, i := range p.after_interceptors {
		i(agent)
	}
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

	defer agent.Session().Get(p.Name + "-wg").(*sync.WaitGroup).Done()

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

func (p *Dispatcher) Kick(key string) {
	p.hub.Kick(key)
}

func (p *Dispatcher) Broadcast(v interface{}, filter func(session NetSession) bool, h ...interface{}) error {
	b, err := Serialize(p.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.Broadcast(b, filter)
	return nil
}

func (p *Dispatcher) BroadcastAll(v interface{}, h ...interface{}) error {
	b, err := Serialize(p.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.BroadcastAll(b)
	return nil
}

func (p *Dispatcher) BroadcastOthers(mySession NetSession, v interface{}, h ...interface{}) error {
	b, err := Serialize(p.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.BroadcastOthers(mySession, b)
	return nil
}

func (p *Dispatcher) BroadcastOne(v interface{}, errFunc func(error), h ...interface{}) error {
	b, err := Serialize(p.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	return p.hub.BroadcastOne(b, errFunc)
}

func (p *Dispatcher) Send(key string, v interface{}) error {
	data, err := Serialize(p.serializer, v)
	if err != nil {
		return err
	}
	p.hub.Send(key, data)
	return nil
}

func (p *Dispatcher) MultiSend(keys []string, v interface{}) error {
	data, err := Serialize(p.serializer, v)
	if err != nil {
		return err
	}
	p.hub.MultiSend(keys, data)
	return nil
}

func (p *Dispatcher) SetGroup(session NetSession, group string) {
	p.hub.SetGroup(session, group)
}

func (p *Dispatcher) Group(group string, v interface{}, h ...interface{}) error {
	b, err := Serialize(p.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.Group(group, b)
	return nil
}

func (p *Dispatcher) GroupOne(group string, v interface{}, errFunc func(error), h ...interface{}) error {
	b, err := Serialize(p.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.GroupOne(group, b, errFunc)
	return nil
}

func (p *Dispatcher) RangeConn(f func(NetConn)) {
	conns := p.hub.Slice()
	for _, conn := range conns {
		f(conn)
	}
}

func (p *Dispatcher) RangeSession(f func(session NetSession)) {
	conns := p.hub.Slice()
	for _, conn := range conns {
		f(conn.Session())
	}
}

func (p *Dispatcher) handleConnectionCreated(agent NetAgent) {
	p.createdChan <- agent
}

func (p *Dispatcher) handleConnectionClosed(agent NetAgent) {
	log4g.Debug("handling closed session")
	wg := new(sync.WaitGroup)
	wg.Add(1)
	agent.Session().Set(p.Name+"-wg", wg)
	p.sessionClosedChan <- agent
	wg.Wait()
	agent.Session().Remove(p.Name + "wg")
	log4g.Debug("handled closed session")
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
