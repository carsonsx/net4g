package net4g

import (
	"sync"
	"time"
	"github.com/carsonsx/log4g"
	"runtime/debug"
	"errors"
	"fmt"
)

type NetHub struct {
	connections      map[string]NetConn
	groupsConn       map[string][]NetConn
	groupsRound      map[string]int
	groupMutex       sync.Mutex
	enableLB         bool
	lbConnections    []NetConn
	lbRound          int
	addChan          chan *chanData
	getChan          chan *chanData
	hasChan          chan *chanData
	sliceChan        chan *chanData
	keyChan          chan *chanData
	removeChan       chan NetConn
	size             int
	heartbeat        bool
	heartbeatTicker  *time.Ticker
	heartbeatTimeout time.Duration
	kickChan         chan func(session NetSession) bool
	broadcastChan    chan *chanData
	closing          chan bool
	closed           bool
	wg               sync.WaitGroup
}

type chanData struct {
	conn    NetConn
	conns    []NetConn
	key     string
	value   interface{}
	one     bool
	group   string
	data    []byte
	filter  func(session NetSession) bool
	once    bool
	errFunc func(err error)
	wg sync.WaitGroup
}

func (hub *NetHub) Start() {
	hub.connections = make(map[string]NetConn)
	hub.groupsConn = make(map[string][]NetConn)
	hub.groupsRound = make(map[string]int)
	hub.addChan = make(chan *chanData, 100)
	hub.keyChan = make(chan *chanData, 100)
	hub.getChan = make(chan *chanData, 1)
	hub.hasChan = make(chan *chanData, 1)
	hub.sliceChan = make(chan *chanData, 1)
	hub.removeChan = make(chan NetConn, 100)
	hub.kickChan = make(chan func(session NetSession) bool, 10)
	hub.broadcastChan = make(chan *chanData, 1000)
	hub.closing = make(chan bool, 1)
	hub.wg.Add(1)
	go func() {
		hub.heartbeatTimeout = NetConfig.HeartbeatFrequency + NetConfig.NetTolerableTime
		hub.heartbeatTicker = time.NewTicker(HEART_BEAT_INTERVAL)
		if !hub.heartbeat {
			hub.heartbeatTicker.Stop()
		}
		for {
			if !hub.do() {
				break
			}
		}
		hub.wg.Done()
	}()
}

func (hub *NetHub) do() (cond bool) {

	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Hub Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			log4g.Error("********************* Hub Panic *********************")
			cond = true
		}
	}()

	select {
	case cData := <-hub.addChan:
		cData.conn.Session().Set(SESSION_CONNECT_KEY, cData.key)
		hub.connections[cData.key] = cData.conn
		if hub.enableLB {
			hub.lbConnections = append(hub.lbConnections, cData.conn)
		}
		hub.size++
		log4g.Debug("connection count: %d", len(hub.connections))
		cData.wg.Done()
	case filter := <-hub.kickChan:
		if filter != nil {
			for _, conn := range hub.connections {
				if filter(conn.Session()) {
					conn.Close()
					hub._delete(conn)
					log4g.Warn("kicked connection %s", conn.RemoteAddr().String())
					break
				}
			}
		}
	case cData := <-hub.keyChan:
		if _, ok := hub.connections[cData.key]; ok {
			log4g.Panic("connection name '%s' existed", cData.key)
		}
		delete(hub.connections, cData.conn.Session().GetString(SESSION_CONNECT_KEY))
		cData.conn.Session().Set(SESSION_CONNECT_KEY, cData.key)
		hub.connections[cData.key] = cData.conn
		log4g.Debug("connection count: %d", len(hub.connections))
	case cData := <-hub.getChan:
		cData.conn = hub.connections[cData.key]
		cData.wg.Done()
	case cData := <-hub.hasChan:
		cData.one = false
		for _, conn := range hub.connections {
			if conn.Session().Get(cData.key) == cData.value {
				cData.one = true
				break
			}
		}
		cData.wg.Done()
	case cData := <-hub.sliceChan:
		for _, conn := range hub.connections {
			cData.conns = append(cData.conns, conn)
		}
		cData.wg.Done()
	case conn := <-hub.removeChan:
		hub._delete(conn)
	case t := <-hub.heartbeatTicker.C:
		for _, conn := range hub.connections {
			if t.UnixNano() > conn.Session().GetInt64(HEART_BEAT_LAST_TIME)+hub.heartbeatTimeout.Nanoseconds() {
				log4g.Warn("client timeout: %s", conn.RemoteAddr().String())
				hub._delete(conn)
				conn.Close()
			}
		}
	case cData := <-hub.broadcastChan:
		if cData.key != "" { // Send to one conn
			hub.connections[cData.key].Write(cData.data)
		} else if cData.one {
			connCount := len(hub.lbConnections)
			if connCount > 0 {
				if hub.lbRound >= connCount {
					hub.lbRound = 0
				}
				log4g.Trace("group size: %d, round robin: %d", connCount, hub.lbRound)
				err := hub.lbConnections[hub.lbRound].Write(cData.data)
				if err != nil &&cData.errFunc != nil {
					cData.errFunc(err)
				}
				hub.lbRound++
			} else if cData.errFunc != nil {
				err := errors.New("not found any connection")
				log4g.Error(err)
				cData.errFunc(err)
			}
		} else if cData.group != "" {// Send to group
			log4g.Debug("send to group %s", cData.group)
			conns := hub.groupsConn[cData.group]
			if cData.once { // send to group's one by round robin
				round := hub.groupsRound[cData.group]
				groupsConnCount := len(conns)
				if groupsConnCount > 0 {
					if round >= groupsConnCount {
						round = 0
					}
					//log4g.Debug("group size: %d, round robin: %d", groupsConnCount, round)
					conns[round].Write(cData.data)
					round++
					hub.groupsRound[cData.group] = round
				} else {
					err := errors.New(fmt.Sprintf("not found any group[%s] connection", cData.group))
					log4g.Error(err)
					if cData.errFunc != nil {
						cData.errFunc(err)
					}
				}
			} else { // send to group's all
				for _, conn := range conns {
					conn.Write(cData.data)
				}
			}
		} else {// broadcast by filter
			//log4g.Debug("broadcast to %d connections", len(hub.connections))
			for _, conn := range hub.connections {
				if cData.filter == nil || cData.filter(conn.Session()) {
					conn.Write(cData.data)
					if log4g.IsDebugEnabled() {
						log4g.Debug("[broadcast] sent to %s", conn.Session().Get(SESSION_ID))
					}
					if cData.once {
						break
					}
				}
			}
		}
	case <-hub.closing:
		return false
	}

	return true
}

func (hub *NetHub) _delete(conn NetConn) {
	delete(hub.connections, conn.Session().GetString(SESSION_CONNECT_KEY))
	for i, _conn := range hub.lbConnections {
		if _conn == conn {
			hub.lbConnections = append(hub.lbConnections[:i], hub.lbConnections[i+1:]...)
			break
		}
	}
	gName := conn.Session().GetString(SESSION_GROUP_NAME)
	if gName != "" {
		hub.groupMutex.Lock()
		groups := hub.groupsConn[gName]
		for i, c := range groups {
			if c == conn {
				hub.groupsConn[gName] = append(groups[:i], groups[i+1:]...)
				break
			}
		}
		hub.groupMutex.Unlock()
	}
	hub.size--
}

func (hub *NetHub) SetKey(conn NetConn, key string) {
	cData := new(chanData)
	cData.conn = conn
	cData.key = key
	hub.keyChan <- cData
}

func (hub *NetHub) Add(key string, conn NetConn) {
	hub.Heartbeat(conn)
	cData := new(chanData)
	cData.key = key
	cData.conn = conn
	cData.wg.Add(1)
	hub.addChan <- cData
	cData.wg.Wait()
}

func (hub *NetHub) Get(key string) NetConn {
	cData := new(chanData)
	cData.key = key
	cData.wg.Add(1)
	hub.getChan <- cData
	cData.wg.Wait()
	return cData.conn
}

func (hub *NetHub) HasSession(k string, v interface{}) bool {
	log4g.Trace("has session: key=%s,val=%s", k, v)
	cData := new(chanData)
	cData.wg.Add(1)
	cData.key = k
	cData.value = v
	hub.hasChan <- cData
	cData.wg.Wait()
	log4g.Trace("has session: %v",  cData.one)
	return cData.one
}

func (hub *NetHub) Remove(conn NetConn) {
	hub.removeChan <- conn
}

func (hub *NetHub) Slice() []NetConn {
	cData := new(chanData)
	cData.wg.Add(1)
	hub.sliceChan <- cData
	cData.wg.Wait()
	return cData.conns
}

func (hub *NetHub) Heartbeat(conn NetConn) {
	if hub.heartbeat {
		conn.Session().Set(HEART_BEAT_LAST_TIME, time.Now().UnixNano())
	}
}

func (hub *NetHub) Kick(filter func(session NetSession) bool) {
	hub.kickChan <- filter
}

func (hub *NetHub) Broadcast(data []byte, filter func(session NetSession) bool) {
	cData := new(chanData)
	cData.data = data
	cData.filter = filter
	hub.broadcastChan <- cData
}

func (hub *NetHub) BroadcastAll(data []byte) {
	hub.Broadcast(data, nil)
}

func (hub *NetHub) BroadcastOthers(mySession NetSession, data []byte) {
	hub.Broadcast(data, func(session NetSession) bool {
		return mySession != session
	})
}

func (hub *NetHub) Someone(data []byte, filter func(session NetSession) bool) {
	cData := new(chanData)
	cData.data = data
	cData.filter = filter
	cData.once = true
	hub.broadcastChan <- cData
}

func (hub *NetHub) SetGroup(session NetSession, group string) {
	if group != "" {
		hub.groupMutex.Lock()
		conn := hub.Get(session.GetString(SESSION_CONNECT_KEY))
		if conn != nil {
			groups := hub.groupsConn[group]
			hub.groupsConn[group] = append(groups, conn)
			session.Set(SESSION_GROUP_NAME, group)
			log4g.Debug("set group %s for %s", group, conn.RemoteAddr().String())
			log4g.Debug("group size: %d", len(hub.groupsConn[group]))
		}
		hub.groupMutex.Unlock()
	}
}

func (hub *NetHub) Group(group string, data []byte) {
	cData := new(chanData)
	cData.group = group
	cData.data = data
	hub.broadcastChan <- cData
}

func (hub *NetHub) GroupOne(group string, data []byte, errFunc func(error)) {
	cData := new(chanData)
	cData.group = group
	cData.data = data
	cData.once = true
	cData.errFunc = errFunc
	hub.broadcastChan <- cData
}

func (hub *NetHub) One(data []byte, errFunc func(error)) error {

	if !hub.enableLB {
		panic("required to enable load balance")
	}

	if hub.closed {
		text := "hub was closed"
		log4g.Error(text)
		return errors.New(text)
	}

	cData := new(chanData)
	cData.one = true
	cData.data = data
	cData.errFunc = errFunc
	hub.broadcastChan <- cData
	return nil
}

func (hub *NetHub) CloseConnections() {
	for _, conn := range hub.connections {
		conn.Close()
	}
	log4g.Debug("closed %d connections", len(hub.connections))
}

func (hub *NetHub) Closed() bool {
	return hub.closed
}


func (hub *NetHub) Destroy() {
	hub.closing <- true
	hub.wg.Wait()
	//close(hub.addChan)
	//close(hub.removeChan)
	//close(hub.broadcastChan)
	//close(hub.closing)
	hub.closed = true
	log4g.Info("closed net hub")
}
