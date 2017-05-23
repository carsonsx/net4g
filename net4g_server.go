package net4g

import (
	"log"
	"net"
	"sync"
	"os"
	"os/signal"
	"time"
)

func NewTcpServer(addr string, serializer ...Serializer) *tcpServer {
	server := new(tcpServer)
	server.Addr = addr
	if len(serializer) > 0 {
		server.serializer = serializer[0]
	} else {
		server.serializer = NewEmptySerializer()
	}
	return server
}

type tcpServer struct {
	Addr        string
	serializer  Serializer
	dispatchers []*dispatcher
	mutex       sync.Mutex
	mgr         *NetManager
	heartbeat   bool
	listener net.Listener
	wg sync.WaitGroup
}

func (s *tcpServer) AddDispatchers(dispatchers ...*dispatcher) *tcpServer {
	for _, d := range dispatchers {
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
	s.mgr.Run()

	s.wg.Add(1)
	go func() {
		s.listen()
		s.wg.Done()
	}()

	return s
}

func (s *tcpServer) listen() {

	delay := 5 * time.Millisecond
	maxDelay := time.Second

	for {
		netconn, err := s.listener.Accept()
		if err != nil {
			log.Println(err)
			if neterr, ok := err.(net.Error); ok && neterr.Temporary() {
				delay = SmartSleep(delay, maxDelay)
				continue
			}
			break
		}
		log.Printf("accept connection from %s", netconn.RemoteAddr().String())
		//new event
		conn := NewNetConn(netconn)
		s.mgr.Add(conn)
		s.wg.Add(1)
		go func() {// one connection, one goroutine to read
			newNetReader(conn, s.serializer, s.dispatchers, s.mgr).Read(nil, func(data []byte) {
				if s.heartbeat && IsHeartbeatData(data) {
					//log.Println("heartbeat callback")
					conn.Write(data) //write back heartbeat
				}
			})
			//close event
			s.mgr.Remove(conn)
			s.wg.Done()
		}()
	}
}

func (s *tcpServer) Broadcast(v interface{}, filter func(session NetSession) bool) error {
	return newNetRes(nil, s.serializer, s.mgr).Broadcast(v, filter)
}

func (s *tcpServer) BroadcastAll(v interface{}) error {
	return newNetRes(nil, s.serializer, s.mgr).BroadcastAll(v)
}

func (s *tcpServer) BroadcastOthers(mySession NetSession, v interface{}) error {
	return newNetRes(nil, s.serializer, s.mgr).BroadcastOthers(mySession, v)
}

func (s *tcpServer) SafeWait()  {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill)
	log.Printf("Server is closing with signal %v\n", <-sig)
	//notify dispatchers the server is closing
	s.listener.Close()
	s.mgr.Close()
	s.wg.Wait()
	log.Println("Client was closed safely")
}