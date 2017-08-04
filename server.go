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
	Name          string
	Addr          string
	serializer    Serializer
	dispatchers   []*dispatcher
	mutex         sync.Mutex
	hub           NetHub
	heartbeat     bool
	listener      net.Listener
	closeConn     sync.WaitGroup
	closeListen   sync.WaitGroup
	started       bool
	statusMonitor bool
}


func (s *tcpServer) SetSerializer(serializer Serializer) *tcpServer {
	s.serializer = serializer
	return s
}

func (s *tcpServer) AddDispatchers(dispatchers ...*dispatcher) *tcpServer {
	for _, d := range dispatchers {
		if d.serializer != nil {
			panic(fmt.Sprintf("dispatcher [%s] has bind with server [%s]", d.Name, s.Name))
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

func (s *tcpServer) EnableStatusMonitor() *tcpServer {
	s.statusMonitor = true
	return s
}

func (s *tcpServer) Start() *tcpServer {

	if s.serializer == nil {
		panic("no serializer")
	}

	if len(s.dispatchers) == 0 {
		panic("no dispatcher")
	}

	if s.statusMonitor {
		go func() {
			for {
				time.Sleep(180 * time.Second)
				log4g.Info("*[Server Status] goroutine  num: %d, connection num: %d", runtime.NumGoroutine(), s.hub.Count())
			}
		}()
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

	s.started = true

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
		agent := newNetAgent(s.hub, conn, nil, nil, s.serializer)
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
	//close listener
	s.closeListen.Add(1)
	s.listener.Close()
	s.closeListen.Wait()
	log4g.Info("closed server[%s] listener", s.Addr)

	//close all connections
	s.hub.CloseConnections()
	s.closeConn.Wait()
	log4g.Info("closed server[%s] connections/readers", s.Addr)

	//close net manager
	s.hub.Destroy()
	log4g.Info("closed server[%s] manager", s.Addr)

	//close dispatchers
	for _, d := range s.dispatchers {
		d.Destroy()
	}
	log4g.Info("closed server[%s] dispatcher", s.Addr)

	log4g.Info("closed server[%s]", s.Addr)
}

func (s *tcpServer) Wait(others ...Closer) {
	sig := make(chan os.Signal, 1)

	signal.Notify(sig, os.Interrupt, os.Kill, Terminal)
	log4g.Info("server[%s] is closing with signal %v", s.Addr, <-sig)

	for _, other := range others {
		other.Close()
	}
	s.Close()
	close(sig)
}
