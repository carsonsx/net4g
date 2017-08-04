package net4g

import (
	"github.com/carsonsx/log4g"
	"reflect"
	"runtime/debug"
	"sync"
)

func Dispatch(dispatchers []*dispatcher, agent NetAgent) {
	found := false
	for _, p := range dispatchers {
		if p.running {
			p.dispatchChan <- agent
			found = true
		}
	}
	if !found {
		log4g.Warn("not found any running dispatcher")
	}
}

func NewDispatcher(name string, goroutineNum int) *dispatcher {
	p := new(dispatcher)
	p.Name = name
	p.createdChan = make(chan NetAgent, 100)
	p.dispatchChan = make(chan NetAgent, 1000)
	p.sessionClosedChan = make(chan NetAgent, 100)
	p.destroyChan = make(chan bool, 1)
	p.msgHandlers = make(map[interface{}]func(agent NetAgent))
	p.goroutineNum = goroutineNum
	p.run()
	log4g.Info("new a %s dispatcher", name)
	return p
}

type dispatcher struct {
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
	goroutineNum             int
	running                  bool
	wg                       sync.WaitGroup
}

func (p *dispatcher) AddHandler(h func(agent NetAgent), key ...interface{}) {
	if len(key) > 0 {
		p.msgHandlers[key[0]] = h
		log4g.Info("dispatcher[%s] added a handler for %v", p.Name, key[0])
	} else {
		p.globalHandlers = append(p.globalHandlers, h)
		log4g.Info("dispatcher[%s] added global handler", p.Name)
	}
}

func (p *dispatcher) OnConnectionCreated(h func(agent NetAgent)) {
	p.connectionCreatedHandlers = append(p.connectionCreatedHandlers, h)
}


func (p *dispatcher) OnConnectionClosed(h func(agent NetAgent)) {
	p.connectionClosedHandlers = append(p.connectionClosedHandlers, h)
}

func (p *dispatcher) OnDestroy(h func()) {
	p.destroyHandler = h
}


func (p *dispatcher) run() {
	p.wg.Add(p.goroutineNum)

	go func() {
		defer p.wg.Done()
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
			case data := <-p.dispatchChan:
				p.dispatch(data)
			}
		}
	}()

	for i := 0; i < p.goroutineNum - 1; i++ {
		go func() {
			defer p.wg.Done()
		outer:
			for {
				select {
				case <-p.destroyChan:
					break outer
				case data := <-p.dispatchChan:
					p.dispatch(data)
				}
			}
		}()
	}

	p.running = true
}


func (p *dispatcher) dispatch(agent NetAgent) {

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

	var key interface{}

	if agent.Msg() == nil {
		if agent.RawPack().Id > 0 {
			key = agent.RawPack().Id
		} else if agent.RawPack().Key != "" {
			key = agent.RawPack().Key
		}
	} else {
		key = reflect.TypeOf(agent.Msg())
	}

	if h, ok := p.msgHandlers[key]; ok {
		log4g.Trace("dispatcher[%s] is dispatching %v to handler", p.Name, key)
		h(agent)
	} else {
		log4g.Trace("dispatcher[%s] not found any handler for %v", p.Name, key)
	}

	for _, i := range p.after_interceptors {
		i(agent)
	}
}


func (p *dispatcher) onConnectionCreatedHandlers(agent NetAgent) {

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


func (p *dispatcher) onConnectionClosedHandlers(agent NetAgent) {

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


func (p *dispatcher) Kick(key string) {
	p.hub.Kick(key)
}

func (p *dispatcher) Broadcast(v interface{}, filter func(session NetSession) bool, prefix ...byte) error {
	b, err := Serialize(p.serializer, v, prefix...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.Broadcast(b, filter)
	return nil
}

func (p *dispatcher) BroadcastAll(v interface{}, prefix ...byte) error {
	b, err := Serialize(p.serializer, v, prefix...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.BroadcastAll(b)
	return nil
}

func (p *dispatcher) BroadcastOthers(mySession NetSession, v interface{}, prefix ...byte) error {
	b, err := Serialize(p.serializer, v, prefix...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.BroadcastOthers(mySession, b)
	return nil
}

func (p *dispatcher) BroadcastOne(v interface{}, errFunc func(error), prefix ...byte) error {
	b, err := Serialize(p.serializer, v, prefix...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	return p.hub.BroadcastOne(b, errFunc)
}

func (p *dispatcher) Send(key string, v interface{}) error {
	data, err := Serialize(p.serializer, v)
	if err != nil {
		return err
	}
	p.hub.Send(key, data)
	return nil
}

func (p *dispatcher) MultiSend(keys []string, v interface{}) error {
	data, err := Serialize(p.serializer, v)
	if err != nil {
		return err
	}
	p.hub.MultiSend(keys, data)
	return nil
}

func (p *dispatcher) SetGroup(session NetSession, group string) {
	p.hub.SetGroup(session, group)
}

func (p *dispatcher) Group(group string, v interface{}, prefix ...byte) error {
	b, err := Serialize(p.serializer, v, prefix...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.Group(group, b)
	return nil
}

func (p *dispatcher) GroupOne(group string, v interface{}, errFunc func(error), prefix ...byte) error {
	b, err := Serialize(p.serializer, v, prefix...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	p.hub.GroupOne(group, b, errFunc)
	return nil
}

func (p *dispatcher) RangeConn(f func(NetConn)) {
	conns := p.hub.Slice()
	for _, conn := range conns {
		f(conn)
	}
}

func (p *dispatcher) RangeSession(f func(session NetSession)) {
	conns := p.hub.Slice()
	for _, conn := range conns {
		f(conn.Session())
	}
}

func (p *dispatcher) handleConnectionCreated(agent NetAgent) {
	p.createdChan <- agent
}

func (p *dispatcher) handleConnectionClosed(agent NetAgent) {
	log4g.Debug("handling closed session")
	wg := new(sync.WaitGroup)
	wg.Add(1)
	agent.Session().Set(p.Name + "-wg", wg)
	p.sessionClosedChan <- agent
	wg.Wait()
	agent.Session().Remove(p.Name + "wg")
	log4g.Debug("handled closed session")
}

func (p *dispatcher) Destroy() {
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
