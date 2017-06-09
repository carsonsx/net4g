package net4g

import (
	"sync"
	"time"
	"github.com/carsonsx/log4g"
	"runtime/debug"
	"errors"
	"fmt"
)

const (
	HEART_BEAT_INTERVAL  = 1 * time.Second
	HEART_BEAT_LAST_TIME = "NET4G__01"
	SESSION_GROUP_NAME = "NET4G__02"
	SESSION_CONNECT_KEY = "NET4G__03"
)

type NetHub struct {
	connections   map[string]NetConn
	groupsConn    map[string][]NetConn
	groupsRound   map[string]int
	groupMutex    sync.Mutex
	addChan       chan NetConn
	keyChan       chan *chanData
	removeChan    chan NetConn
	heartbeat     bool
	heartbeatTicker  *time.Ticker
	heartbeatTimeout time.Duration
	kickChan      chan func(session NetSession) bool
	broadcastChan chan *chanData
	closing       chan bool
	wg            sync.WaitGroup
}

type chanData struct {
	conn NetConn
	key string
	group string
	data   []byte
	filter func(session NetSession) bool
	once   bool
	errFunc func(err error)
}

func (hub *NetHub) Start() {
	hub.connections = make(map[string]NetConn)
	hub.groupsConn = make(map[string][]NetConn)
	hub.groupsRound = make(map[string]int)
	hub.addChan = make(chan NetConn, 100)
	hub.keyChan = make(chan *chanData, 100)
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
	case conn := <-hub.addChan:
		conn.Session().Set(SESSION_CONNECT_KEY, conn.RemoteAddr().String())
		hub.connections[conn.RemoteAddr().String()] = conn
		log4g.Debug("connection count: %d", len(hub.connections))
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
		delete(hub.connections, cData.conn.Session().GetString(SESSION_CONNECT_KEY))
		cData.conn.Session().Set(SESSION_CONNECT_KEY, cData.key)
		hub.connections[cData.key] = cData.conn
		log4g.Debug("connection count: %d", len(hub.connections))
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
		} else if cData.group != "" {// Send to group
			conns := hub.groupsConn[cData.group]
			if cData.once { // send to group's one by round robin
				round := hub.groupsRound[cData.group]
				groupsConnLen := len(conns)
				if groupsConnLen > 0 {
					if round >= groupsConnLen {
						round = 0
					}
					//log4g.Debug("group size: %d, round robin: %d", groupsConnLen, round)
					conns[round].Write(cData.data)
					round++
					hub.groupsRound[cData.group] = round
				} else if cData.errFunc != nil {
					cData.errFunc(errors.New(fmt.Sprintf("not found any group[%s] connection", cData.group)))
				}
			} else { // send to group's all
				for _, conn := range conns {
					conn.Write(cData.data)
				}
			}
		} else {// broadcast by filter
			for _, conn := range hub.connections {
				if cData.filter == nil || cData.filter(conn.Session()) {
					conn.Write(cData.data)
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
}

func (hub *NetHub) SetGroup(conn NetConn, group string) {
	if group != "" {
		hub.groupMutex.Lock()
		conn.Session().Set(SESSION_GROUP_NAME, group)
		groups := hub.groupsConn[group]
		hub.groupsConn[group] = append(groups, conn)
		hub.groupMutex.Unlock()
		log4g.Debug("set group %s for %s", group, conn.RemoteAddr().String())
		log4g.Debug("group size: %d", len(hub.groupsConn[group]))
	}
}

func (hub *NetHub) SetKey(conn NetConn, key string) {
	cData := new(chanData)
	cData.conn = conn
	cData.key = key
	hub.keyChan <- cData
}

func (hub *NetHub) Add(conn NetConn) {
	hub.Heartbeat(conn)
	hub.addChan <- conn
}

func (hub *NetHub) Remove(conn NetConn) {
	hub.removeChan <- conn
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

func (hub *NetHub) Range(h func(conn NetConn)) {
	for _, conn := range hub.connections {
		h(conn)
	}
}

func (hub *NetHub) SendToGroup(group string, data []byte) {
	cData := new(chanData)
	cData.group = group
	cData.data = data
	hub.broadcastChan <- cData
}

func (hub *NetHub) SendToGroupOne(group string, data []byte, errFunc func(error)) {
	cData := new(chanData)
	cData.group = group
	cData.data = data
	cData.once = true
	cData.errFunc = errFunc
	hub.broadcastChan <- cData
}

func (hub *NetHub) CloseConnections() {
	for _, conn := range hub.connections {
		conn.Close()
	}
	log4g.Debug("closed %d connections", len(hub.connections))
}

func (hub *NetHub) Destroy() {
	hub.closing <- true
	hub.wg.Wait()
	close(hub.addChan)
	close(hub.removeChan)
	close(hub.broadcastChan)
	close(hub.closing)
	log4g.Info("closed net hub")
}
