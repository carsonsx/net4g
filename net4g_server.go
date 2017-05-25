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

func NewTcpServer(name, addr string, serializer ...Serializer) *tcpServer {
	server := new(tcpServer)
	server.Name = name
	server.Addr = addr
	if len(serializer) > 0 {
		server.serializer = serializer[0]
	} else {
		server.serializer = NewEmptySerializer()
	}
	return server
}

type tcpServer struct {
	Name        string
	Addr        string
	serializer  Serializer
	dispatchers []*dispatcher
	mutex       sync.Mutex
	mgr         *NetManager
	heartbeat   bool
	listener    net.Listener
	closeConn   sync.WaitGroup
	closeListen sync.WaitGroup
	started     bool
}

func (s *tcpServer) AddDispatchers(dispatchers ...*dispatcher) *tcpServer {
	for _, d := range dispatchers {
		if d.serializer != nil {
			panic(fmt.Sprintf("dispatcher [%s] has bind with server [%s]", d.Name, s.Name))
		}
		d.serializer = s.serializer
		d.mgr = s.mgr
		s.dispatchers = append(s.dispatchers, d)
	}
	return s
}

func (s *tcpServer) EnableHeartbeat() *tcpServer {
	s.heartbeat = true
	return s
}

func (s *tcpServer) Start() *tcpServer {

	var err error
	s.listener, err = net.Listen("tcp", s.Addr)
	if err != nil {
		panic(err)
	}
	log4g.Info("TCP server listening on %s", s.listener.Addr().String())

	// Init the connection manager
	s.mgr = new(NetManager)
	s.mgr.heartbeat = s.heartbeat
	s.mgr.Start()

	for _, d := range s.dispatchers {
		d.serializer = s.serializer
		d.mgr = s.mgr
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
		conn := NewNetConn(netconn)
		s.mgr.Add(conn)
		s.closeConn.Add(1)
		go func() { // one connection, one goroutine to read
			newNetReader(conn, s.serializer, s.dispatchers, s.mgr).Read(nil, func(data []byte) bool {
				if IsHeartbeatData(data) {
					log4g.Trace("heartbeat from client")
					s.mgr.Heartbeat(conn)
					conn.Write(data) //write back heartbeat
					return false
				}
				return true
			})
			//close event
			s.mgr.Remove(conn)
			for _, d := range s.dispatchers {
				d.CloseSession(conn.Session())
			}
			log4g.Info("disconnected from %s", conn.RemoteAddr().String())
			s.closeConn.Done()
		}()
	}
}

func (s *tcpServer) close() {
	//close listener
	s.closeListen.Add(1)
	s.listener.Close()
	s.closeListen.Wait()
	log4g.Info("closed server[%s] listener", s.Addr)

	//close all connections
	s.mgr.CloseConnections()
	s.closeConn.Wait()
	log4g.Info("closed server[%s] connections/readers", s.Addr)

	//close net manager
	s.mgr.Close()
	log4g.Info("closed server[%s] manager", s.Addr)

	//close dispatchers
	for _, d := range s.dispatchers {
		d.Close()
	}
	log4g.Info("closed server[%s] dispatcher", s.Addr)

	log4g.Info("closed server[%s]", s.Addr)
}

func (s *tcpServer) Wait(others ...*tcpServer) {
	sig := make(chan os.Signal, 1)
<<<<<<< HEAD
	signal.Notify(sig, os.Interrupt, os.Kill) // created 1 goroutine
	log.Printf("server[%s] is closing with signal %v\n", s.Addr, <-sig)
=======
	signal.Notify(sig, os.Interrupt, os.Kill)
	log4g.Info("server[%s] is closing with signal %v", s.Addr, <-sig)
>>>>>>> 8b42be2e3658e089bc1ca664c30993f1bba16430
	for _, other := range others {
		other.close()
	}
	s.close()
<<<<<<< HEAD
	close(sig)
}
=======
}
>>>>>>> 8b42be2e3658e089bc1ca664c30993f1bba16430
