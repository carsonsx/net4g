package net4g

import (
	"log"
	"sync"
	"time"
)

const (
	HEART_BEAT_INTERVAL  = 1 * time.Second
	HEART_BEAT_LAST_TIME = "__HEART_BEAT_LAST_TIME"
)

type NetManager struct {
	connections        map[NetConn]struct{}
	addChan            chan NetConn
	removeChan         chan NetConn
	heartbeat          bool
	broadcastChan      chan *broadcastData
	closing            chan bool
	wg                 sync.WaitGroup
}

type broadcastData struct {
	data   []byte
	filter func(session NetSession) bool
}

func (m *NetManager) Start() {
	m.connections = make(map[NetConn]struct{})
	m.addChan = make(chan NetConn, 100)
	m.removeChan = make(chan NetConn, 100)
	m.broadcastChan = make(chan *broadcastData, 1000)
	m.closing = make(chan bool, 1)
	m.wg.Add(1)
	go func() {
		heartbeatTimeout := NetConfig.HeartbeatFrequency + NetConfig.NetTolerableTime
		heartbeatTimer := time.NewTicker(HEART_BEAT_INTERVAL)
		if !m.heartbeat {
			heartbeatTimer.Stop()
		}
	outer:
		for {
			select {
			case conn := <-m.addChan:
				m.connections[conn] = struct{}{}
			case conn := <-m.removeChan:
				delete(m.connections, conn)
			case t := <-heartbeatTimer.C:
				for conn := range m.connections {
					if t.UnixNano() > conn.Session().GetInt64(HEART_BEAT_LAST_TIME)+heartbeatTimeout.Nanoseconds() {
						log.Println(t.UnixNano())
						log.Println(conn.Session().GetInt64(HEART_BEAT_LAST_TIME))
						log.Printf("client timeout: %s\n", conn.RemoteAddr().String())
						delete(m.connections, conn)
						conn.Close()
					}
				}
			case bcData := <-m.broadcastChan:
				for conn := range m.connections {
					if bcData.filter == nil || bcData.filter(conn.Session()) {
						conn.Write(bcData.data)
					}
				}
			case <-m.closing:
				break outer
			}
		}
		log.Println("ended manager gorutine")
		m.wg.Done()
	}()
}

func (m *NetManager) Add(conn NetConn) {
	m.Heartbeat(conn)
	m.connections[conn] = struct{}{}
}

func (m *NetManager) Remove(conn NetConn) {
	m.removeChan <- conn
}

func (m *NetManager) Heartbeat(conn NetConn) {
	if m.heartbeat {
		conn.Session().Set(HEART_BEAT_LAST_TIME, time.Now().UnixNano())
	}
}

func (m *NetManager) Broadcast(data []byte, filter func(session NetSession) bool) {
	bcData := new(broadcastData)
	bcData.data = data
	bcData.filter = filter
	m.broadcastChan <- bcData
}

func (m *NetManager) BroadcastAll(data []byte) {
	m.Broadcast(data, nil)
}

func (m *NetManager) BroadcastOthers(mySession NetSession, data []byte) {
	m.Broadcast(data, func(session NetSession) bool {
		return mySession != session
	})
}

func (m *NetManager) CloseConnections() {
	for conn := range m.connections {
		conn.Close()
	}
}

func (m *NetManager) Close() {
	m.closing <- true
	m.wg.Wait()
	close(m.addChan)
	close(m.removeChan)
	close(m.broadcastChan)
	close(m.closing)
	log.Println("closed manager channels")
}
