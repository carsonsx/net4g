package net4g

import (
	"errors"
	"github.com/carsonsx/log4g"
	"sync"
	"time"
	"fmt"
	"github.com/orcaman/concurrent-map"
)

type HeartbeatMode int

const (
	HEART_BEAT_MODE_NONE HeartbeatMode = iota
	HEART_BEAT_MODE_ACTIVE
	HEART_BEAT_MODE_PASSIVE
)

func NewNetHub(heartbeatMode HeartbeatMode, lb bool) *netHub {
	hub := new(netHub)
	hub.connections.m = cmap.New()
	hub.heartbeat(heartbeatMode)
	hub.enableLB = lb
	return hub
}

type NetHub interface {
	Add(key string, conn NetConn)
	Key(conn NetConn, key string)
	Get(key string) NetConn
	Remove(conn NetConn) bool
	Slice() []NetConn
	Count() int
	Heartbeat(conn NetConn)
	Kick(key string) bool
	SetSerializer(serializer Serializer)
	Broadcast(v interface{}, filter func(session NetSession) bool, h ...interface{}) error
	BroadcastAll(v interface{}, h ...interface{}) error
	BroadcastOthers(mySession NetSession, v interface{}, h ...interface{}) error
	BroadcastOne(v interface{}, h ...interface{}) error
	Send(key string, v interface{}) error
	MultiSend(keys []string, v interface{}) error
	SetGroup(session NetSession, group string)
	Group(name string, v interface{}, h ...interface{}) error
	GroupOne(name string, v interface{}, h ...interface{}) error
	RangeConn(f func(NetConn))
	RangeSession(f func(session NetSession))
	Statistics()
	PackCount() (totalRead int64, totalWriting int64, totalWritten int64, popRead int64, popWritten int64)
	DataUsage() (totalRead int64, totalWritten int64, popRead int64, popWritten int64)
	CloseAllConnections()
}

type netConnMap struct {
	m cmap.ConcurrentMap
}

func (m *netConnMap) Get(key string) NetConn {
	if v, ok := m.m.Get(key); ok {
		return v.(NetConn)
	}
	return nil
}

func (m *netConnMap) Range(f func(key string, conn NetConn) bool) {
	ch := m.m.IterBuffered()
	for t := range ch {
		if !f(t.Key, t.Val.(NetConn)) {
			break
		}
	}
}

func (m *netConnMap) RangeAll(f func(key string, conn NetConn)) {
	ch := m.m.IterBuffered()
	for t := range ch {
		f(t.Key, t.Val.(NetConn))
	}
}

type GroupConns struct {
	mutex           sync.RWMutex
	m               map[string][]NetConn
	roundRobinIndex map[string]int
}

func (g *GroupConns) Append(name string, conn NetConn) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if g.m == nil {
		g.m = make(map[string][]NetConn)
	}
	conns := g.m[name]
	conns = append(conns, conn)
	g.m[name] = conns
}

func (g *GroupConns) Remove(name string, conn NetConn) bool {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	if g.m == nil {
		return false
	}
	conns := g.m[name]
	if conns == nil {
		return false
	}
	for i, c := range conns {
		if c == conn {
			conns = append(conns[:i], conns[i+1:]...)
			g.m[name] = conns
			return true
		}
	}
	return false
}

func (g *GroupConns) Range(name string, f func(conn NetConn) bool) {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	if g.m == nil {
		return
	}
	conns := g.m[name]
	if conns == nil {
		return
	}
	for _, c := range conns {
		if !f(c) {
			break
		}
	}
}

func (g *GroupConns) RoundRobin(name string) NetConn {
	g.mutex.RLock()
	defer g.mutex.RUnlock()
	if g.m == nil {
		return nil
	}
	if g.roundRobinIndex == nil {
		g.roundRobinIndex = make(map[string]int)
	}
	conns := g.m[name]
	if conns == nil || len(conns) == 0 {
		return nil
	}
	index := g.roundRobinIndex[name]
	index++
	if index > len(conns)-1 {
		index = 0
	}
	return conns[index]
}

type connSlice struct {
	mutex sync.Mutex
	conns []NetConn
}

func (s *connSlice) Append(conn NetConn) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.conns = append(s.conns, conn)
}

func (s *connSlice) Remove(conn NetConn) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	for i, c := range s.conns {
		if c == conn {
			s.conns = append(s.conns[:i], s.conns[i+1:]...)
			return true
		}
	}
	return false
}

type netHub struct {
	connections           netConnMap
	groupConns            GroupConns
	enableLB              bool
	lbConnections         connSlice
	serializer            Serializer
	totalReadCount        int64
	totalWrittenCount     int64
	popReadCount          int64
	popWrittenCount       int64
	totalWritingCount     int64
	totalReadDataUsage    int64
	totalWrittenDataUsage int64
	popReadDataUsage      int64
	popWriteDataUsage     int64
}


func (hub *netHub) Add(key string, conn NetConn) {
	hub.Heartbeat(conn)
	conn.Session().Set(SESSION_CONNECT_KEY, key)
	hub.connections.m.Set(key, conn)
	if hub.enableLB {
		hub.lbConnections.Append(conn)
	}
	log4g.Info("add new connection %s to hub", key)
}

func (hub *netHub) Key(conn NetConn, key string) {
	if conn.Session().Get(SESSION_CONNECT_KEY) != key {
		if c := hub.connections.Get(key); c != nil {
			log4g.Panic("connection name '%s' existed", key)
		}
		hub.connections.m.Remove(conn.Session().GetString(SESSION_CONNECT_KEY))
		hub.connections.m.Set(key, conn)
		conn.Session().Set(SESSION_CONNECT_KEY, key)
		conn.Session().Set(SESSION_CONNECT_KEY_USER, key)
		log4g.Debug("set connection key with %s", key)
	}
}

func (hub *netHub) Get(key string) NetConn {
	return hub.connections.Get(key)
}

func (hub *netHub) Remove(conn NetConn) bool {
	c := hub.connections.Get(conn.Session().GetString(SESSION_CONNECT_KEY))
	if c == nil {
		return false
	}
		hub.connections.m.Remove(conn.Session().GetString(SESSION_CONNECT_KEY))
	if hub.enableLB {
		hub.lbConnections.Remove(conn)
	}

	gName := conn.Session().GetString(SESSION_GROUP_NAME)
	if gName != "" {
		if hub.groupConns.Remove(gName, conn) {
			log4g.Debug("remove connection %s from group %s", conn.Session().GetString(SESSION_CONNECT_KEY), gName)
		}
	}
	log4g.Info("removed connection: %s", conn.Session().Get(SESSION_CONNECT_KEY))
	log4g.Info("connection count: %d", hub.connections.m.Count())
	return true

}

func (hub *netHub) Slice() []NetConn {
	var conns []NetConn
	hub.connections.RangeAll(func(key string, conn NetConn) {
		conns = append(conns, conn)
	})
	return conns
}

func (hub *netHub) Count() int {
	return hub.connections.m.Count()
}


func (hub *netHub) heartbeat(mode HeartbeatMode) {

	if mode == HEART_BEAT_MODE_NONE {
		return
	}

	//if mode == HEART_BEAT_MODE_ACTIVE {
	//	timer := time.NewTicker(NetConfig.NetTolerableTime * time.Second)
	//	var lastBeatTime int64
	//	go func() {
	//		for {
	//			select {
	//			case t := <-timer.C:
	//				if lastBeatTime > 0 {
	//					hub.connections.RangeAll(func(key string, conn NetConn) {
	//						if conn.Session().GetInt64(HEART_BEAT_TIME) < lastBeatTime {
	//							log4g.Warn("client timeout: %s", conn.RemoteAddr().String())
	//							hub.Remove(conn)
	//							conn.Close()
	//						}
	//					})
	//				}
	//				log4g.Debug("heart beat...")
	//				hub.BroadcastAll(NetConfig.HeartbeatData)
	//				lastBeatTime = t.UnixNano()
	//			}
	//		}
	//	}()
	//}

	go func() {
		heartbeatTimeout := (NetConfig.NetTolerableTime + NetConfig.HeartbeatFrequency) * time.Second
		heartbeatTicker := time.NewTicker(NetConfig.HeartbeatFrequency * time.Second)
		for {
			select {
			case t := <-heartbeatTicker.C:
				hub.connections.RangeAll(func(key string, conn NetConn) {
					now := t.UnixNano()
					beattime := conn.Session().GetInt64(HEART_BEAT_TIME)
					timeout := beattime+heartbeatTimeout.Nanoseconds()
					if now > timeout{
						log4g.Warn("connection timeout: %s", conn.RemoteAddr().String())
						log4g.Debug("now=%v,beattime=%v,timeout=%v", t, time.Unix(beattime/int64(time.Second), beattime%int64(time.Second)), time.Unix(timeout/int64(time.Second), timeout%int64(time.Second)))
						hub.Remove(conn)
						conn.Close()
					} else if mode == HEART_BEAT_MODE_ACTIVE {
						log4g.Debug("local %s heart beat to remote %s", conn.LocalAddr(), conn.RemoteAddr())
						conn.Write(NetConfig.HeartbeatData)
					}
				})
			}
		}
	}()
}

func (hub *netHub) Heartbeat(conn NetConn) {
	log4g.Debug("remote %s heartbeat to local %s", conn.RemoteAddr(), conn.LocalAddr())
	conn.Session().Set(HEART_BEAT_TIME, time.Now().UnixNano())
}

func (hub *netHub) Kick(key string) bool {
	if conn := hub.connections.Get(key); conn != nil {
		hub.connections.m.Remove(key)
		log4g.Warn("kicked connection %s", conn.RemoteAddr().String())
		log4g.Info("connection count: %d", hub.connections.m.Count())
		return true
	}
	return false
}

func (hub *netHub) SetSerializer(serializer Serializer) {
	hub.serializer = serializer
}

func (hub *netHub) Broadcast(v interface{}, filter func(session NetSession) bool, h ...interface{}) error {
	return hub.broadcast(v, filter, false, h...)
}

func (hub *netHub) broadcast(v interface{}, filter func(session NetSession) bool, once bool, h ...interface{}) error {
	if hub.connections.m.Count() <= 0 {
		return nil
	}
	data, err := Serialize(hub.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	hub.connections.Range(func(key string, conn NetConn) bool {
		if filter == nil || filter(conn.Session()) {
			conn.Write(data)
			if log4g.IsDebugEnabled() {
				//log4g.Debug("[broadcast] sent to %s", conn.Session().Get(SESSION_ID))
			}
			if once {
				return false
			}
		}
		return true
	})

	return nil
}

func (hub *netHub) BroadcastAll(v interface{}, h ...interface{}) error {
	return hub.Broadcast(v, nil)
}

func (hub *netHub) BroadcastOthers(mySession NetSession, v interface{}, h ...interface{}) error {
	return hub.Broadcast(v, func(session NetSession) bool {
		return mySession != session
	})
}

func (hub *netHub) BroadcastOne(v interface{}, h ...interface{}) error {
	if !hub.enableLB {
		panic("required to enable load balance")
	}
	return hub.broadcast(v, nil, true, h...)
}

func (hub *netHub) Send(key string, v interface{}) error {
	data, err := Serialize(hub.serializer, v)
	if err != nil {
		return err
	}
	if conn := hub.connections.Get(key); conn != nil {
		err = conn.Write(data)
	} else {
		err = errors.New(fmt.Sprintf("connection %s not found", key))
		log4g.Warn(err)
	}
	return err
}

func (hub *netHub) MultiSend(keys []string, v interface{}) error {
	data, err := Serialize(hub.serializer, v)
	if err != nil {
		return err
	}
	for _, key := range keys {
		if conn := hub.connections.Get(key); conn != nil {
			if err = conn.Write(data); err != nil {
				return err
			}
		}
	}
	return nil
}

func (hub *netHub) SetGroup(session NetSession, name string) {
	if name != "" {
		conn := hub.Get(session.GetString(SESSION_CONNECT_KEY))
		if conn != nil {
			hub.groupConns.Append(name, conn)
			session.Set(SESSION_GROUP_NAME, name)
			log4g.Debug("set group %s for %s", name, conn.RemoteAddr().String())
			log4g.Debug("group size: %d", len(hub.groupConns.m))
		}
	}
}

func (hub *netHub) Group(name string, v interface{}, h ...interface{}) error {
	data, err := Serialize(hub.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	hub.groupConns.Range(name, func(conn NetConn) bool {
		if err = conn.Write(data); err != nil {
			return false
		}
		return true
	})
	return err
}

func (hub *netHub) GroupOne(name string, v interface{}, h ...interface{}) error {
	log4g.Debug("send to group %s", name)
	data, err := Serialize(hub.serializer, v, h...)
	if err != nil {
		log4g.Error(err)
		return err
	}
	conn := hub.groupConns.RoundRobin(name)
	if conn == nil {
		err := errors.New("not found any connection")
		log4g.Error(err)
		return err
	}
	return conn.Write(data)
}

func (hub *netHub) RangeConn(f func(NetConn)) {
	conns := hub.Slice()
	for _, conn := range conns {
		f(conn)
	}
}

func (hub *netHub) RangeSession(f func(session NetSession)) {
	conns := hub.Slice()
	for _, conn := range conns {
		f(conn.Session())
	}
}

func (hub *netHub) Statistics() {
	hub.totalWritingCount = 0
	hub.connections.RangeAll(func(key string, conn NetConn) {
		rc, wc := conn.PopCount()
		hub.totalReadCount += rc
		hub.totalWrittenCount += wc
		hub.popReadCount += rc
		hub.popWrittenCount += wc
		rdu, wdu := conn.PopDataUsage()
		hub.totalReadDataUsage += rdu
		hub.totalWrittenDataUsage += wdu
		hub.popReadDataUsage += rdu
		hub.popWriteDataUsage += wdu
		hub.totalWritingCount += conn.WritingCount()
	})
}

func (hub *netHub) PackCount() (totalRead int64, totalWriting int64, totalWritten int64, popRead int64, popWritten int64) {
	totalRead = hub.totalReadCount
	totalWriting = hub.totalWritingCount
	totalWritten = hub.totalWrittenCount
	popRead = hub.popReadCount
	hub.popReadCount = 0
	popWritten = hub.popWrittenCount
	hub.popWrittenCount = 0
	return
}

func (hub *netHub) DataUsage() (totalRead int64, totalWritten int64, popRead int64, popWritten int64) {
	totalRead = hub.totalReadDataUsage
	totalWritten = hub.totalWrittenDataUsage
	popRead = hub.popReadDataUsage
	hub.popReadDataUsage = 0
	popWritten = hub.popWriteDataUsage
	hub.popWriteDataUsage = 0
	return
}

func (hub *netHub) CloseAllConnections() {
	count := 0
	hub.connections.RangeAll(func(key string, conn NetConn) {
		conn.Close()
		count++
	})
	log4g.Info("closed all %d connections", count)
}
