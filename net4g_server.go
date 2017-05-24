package net4g

import (
	"log"
	"net"
	"sync"
	"os"
	"os/signal"
	"time"
	"fmt"
	"github.com/carsonsx/net4g/util"
	"runtime"
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
	log.Printf("TCP server listening on %s", s.listener.Addr().String())

	// Init the connection manager
	s.mgr = new (NetManager)
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
		log.Printf("total goroutine: %d", runtime.NumGoroutine())
		netconn, err := s.listener.Accept()
		if err != nil {
			log.Println(err)
			if neterr, ok := err.(net.Error); ok && neterr.Temporary() {
				delay = util.SmartSleep(delay, maxDelay)
				continue
			}
			break
		}
		log.Printf("accept connection from %s", netconn.RemoteAddr().String())
		//new event
		conn := NewNetConn(netconn)
		s.mgr.Add(conn)
		s.closeConn.Add(1)
		go func() {// one connection, one goroutine to read
			newNetReader(conn, s.serializer, s.dispatchers, s.mgr).Read(nil, func(data []byte) {
				if s.heartbeat && IsHeartbeatData(data) {
					//log.Println("heartbeat callback")
					conn.Write(data) //write back heartbeat
				}
			})
			//close event
			s.mgr.Remove(conn)
			for _, d := range s.dispatchers {
				d.CloseSession(conn.Session())
			}
			log.Printf("disconnected from %s", conn.RemoteAddr().String())
			s.closeConn.Done()
		}()
	}
}

func (s *tcpServer) close()  {
	//close listener
	s.closeListen.Add(1)
	s.listener.Close()
	s.closeListen.Wait()
	log.Printf("closed server[%s] listener", s.Addr)

	//close all connections
	s.mgr.CloseConnections()
	log.Printf("closed server[%s] connections/readers", s.Addr)
	s.closeConn.Wait()

	//close net manager
	s.mgr.Close()
	log.Printf("closed server[%s] manager", s.Addr)

	//close dispatchers
	for _, d := range s.dispatchers {
		d.Close()
	}
	log.Printf("closed server[%s] dispatcher", s.Addr)

	log.Printf("closed server[%s]", s.Addr)
}

func (s *tcpServer) Wait(others ...*tcpServer)  {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill)
	log.Printf("server[%s] is closing with signal %v\n", s.Addr, <-sig)
	for _, other := range others {
		other.close()
	}
	s.close()
}