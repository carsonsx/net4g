package net4g

import (
	"fmt"
	"github.com/carsonsx/log4g"
	"github.com/carsonsx/net4g/util"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"
)

func NewTcpServer(name, addr string) *tcpServer {
	server := new(tcpServer)
	server.Name = name
	server.Addr = addr
	return server
}

type tcpServer struct {
	Name        string
	Addr        string
	serializer  Serializer
	dispatchers []*Dispatcher
	mutex       sync.Mutex
	hub         NetHub
	heartbeat   bool
	listener    net.Listener
	closeConn   sync.WaitGroup
	closeListen sync.WaitGroup
	closed     bool
	monitor     bool
	monitorLog  *log4g.Loggers
	startTime time.Time
}

func (s *tcpServer) SetSerializer(serializer Serializer) *tcpServer {
	s.serializer = serializer
	return s
}

func (s *tcpServer) AddDispatchers(dispatchers ...*Dispatcher) *tcpServer {
	for _, d := range dispatchers {
		if d.serializer != nil {
			panic(fmt.Sprintf("Dispatcher [%s] has bind with server [%s]", d.Name, s.Name))
		}
		d.serializer = s.serializer
		d.hub = s.hub
		s.dispatchers = append(s.dispatchers, d)
	}
	return s
}

func (s *tcpServer) EnableHeartbeat() *tcpServer {
	s.heartbeat = true
	return s
}

func (s *tcpServer) EnableMonitor(monitorLog *log4g.Loggers) *tcpServer {
	s.monitor = true
	s.monitorLog = monitorLog
	return s
}

func (s *tcpServer) Start() *tcpServer {

	if s.serializer == nil {
		panic("no serializer")
	}

	if len(s.dispatchers) == 0 {
		panic("no Dispatcher")
	}

	var err error
	s.listener, err = net.Listen("tcp", s.Addr)
	if err != nil {
		panic(err)
	}
	log4g.Info("TCP server listening on %s", s.listener.Addr().String())

	// Init the connection manager
	s.hub = NewNetHub(s.heartbeat, false)

	for _, d := range s.dispatchers {
		d.serializer = s.serializer
		d.hub = s.hub
	}

	go func() {
		s.listen()
		s.closeListen.Done()
	}()

	s.startTime = time.Now()

	if s.monitor {
		go func() {
			ticker := time.NewTicker(time.Duration(NetConfig.MonitorBeat) * time.Second)
			previousTime := s.startTime
			for {
				if closed {
					ticker.Stop()
					break
				}
				select {
				case <- ticker.C:
					trc, twc, prc, pwc := s.hub.PackCount()
					now := time.Now()
					duration := now.Sub(previousTime)
					previousTime = now
					s.monitorLog.Info("")
					s.monitorLog.Info("*[%s status] goroutine: %d, connection: %d", s.Name, runtime.NumGoroutine(), s.hub.Count())
					s.monitorLog.Info("*[%s message] read: %d, write: %d", s.Name, trc, twc)
					s.monitorLog.Info("*[%s per sec] read: %d, write: %d", s.Name, prc * int64(time.Second) / int64(duration), pwc * int64(time.Second) / int64(duration))
				}
			}
		}()
	}

	return s
}

func (s *tcpServer) listen() {

	delay := 5 * time.Millisecond
	maxDelay := time.Second

	for {
		if log4g.IsTraceEnabled() {
			log4g.Info("total goroutine: %d", runtime.NumGoroutine())
		}
		netconn, err := s.listener.Accept()
		if err != nil {
			log4g.Error(err)
			if neterr, ok := err.(net.Error); ok && neterr.Temporary() {
				delay = util.SmartSleep(delay, maxDelay)
				continue
			}
			break
		}
		log4g.Info("accept connection from %s", netconn.RemoteAddr().String())
		//new event
		conn := newTcpConn(netconn)
		s.hub.Add(conn.RemoteAddr().String(), conn)
		agent := newNetAgent(s.hub, conn, nil, nil, nil, s.serializer)
		for _, d := range s.dispatchers {
			d.handleConnectionCreated(agent)
		}

		go func() { // one connection, one goroutine to read
			s.closeConn.Add(1)
			defer s.closeConn.Done()
			newNetReader(conn, s.serializer, s.dispatchers, s.hub).Read(func(data []byte) bool {
				if IsHeartbeatData(data) {
					log4g.Trace("heartbeat from client")
					s.hub.Heartbeat(conn)
					conn.Write(data) //write back heartbeat
					return false
				}
				return true
			})
			//close event
			s.hub.Remove(conn)
			for _, d := range s.dispatchers {
				d.handleConnectionClosed(agent)
			}
			log4g.Info("disconnected from %s", conn.RemoteAddr().String())
		}()
	}
}

func (s *tcpServer) Close() {

	s.closed = true

	//close listener
	s.closeListen.Add(1)
	s.listener.Close()
	s.closeListen.Wait()
	log4g.Info("closed server[%s] listener", s.Name)

	//close all connections
	s.hub.CloseConnections()
	s.closeConn.Wait()
	log4g.Info("closed server[%s] connections/readers", s.Name)

	//close net manager
	s.hub.Destroy()
	log4g.Info("closed server[%s] manager", s.Name)

	//close dispatchers
	for _, d := range s.dispatchers {
		d.Destroy()
	}
	log4g.Info("closed server[%s] Dispatcher", s.Name)

	log4g.Info("closed server[%s]", s.Name)
}

func (s *tcpServer) Wait(others ...Closer) {
	sig := make(chan os.Signal, 1)

	signal.Notify(sig, os.Interrupt, os.Kill, Terminal)
	log4g.Info("server[%s] is closing with signal %v", s.Name, <-sig)

	for _, other := range others {
		other.Close()
	}
	s.Close()
	close(sig)
}
