package net4g

import (
	"fmt"
	"github.com/carsonsx/gutil"
	"github.com/carsonsx/log4g"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"
)

func NewTcpServer(addr ...string) *TCPServer {
	return NewNamedTcpServer(NetConfig.ServerName, addr...)
}

func NewNamedTcpServer(name string, addr ...string) *TCPServer {
	server := new(TCPServer)
	server.Name = name
	if len(addr) > 0 {
		server.Addr = addr[0]
	} else {
		server.Addr = NetConfig.Address
	}
	return server
}

type TCPServer struct {
	Name             string
	Addr             string
	readIntercepter  Intercepter
	writeIntercepter Intercepter
	Serializer       Serializer
	Dispatcher       *Dispatcher
	dispatchers      []*Dispatcher
	mutex            sync.Mutex
	hub              NetHub
	listener         net.Listener
	closeConn        sync.WaitGroup
	closeListen      sync.WaitGroup
	closed           bool
	monitor          bool
	monitorLog       *log4g.Loggers
	startTime        time.Time
}

func (s *TCPServer) SetSerializer(serializer Serializer) *TCPServer {
	s.Serializer = serializer
	return s
}

func (s *TCPServer) NewJsonSerializer() *TCPServer {
	s.Serializer = NewJsonSerializer()
	return s
}

func (s *TCPServer) AddDispatchers(dispatchers ...*Dispatcher) *TCPServer {
	for _, d := range dispatchers {
		if d.serializer != nil {
			panic(fmt.Sprintf("Dispatcher [%s] has bind with server [%s]", d.Name, s.Name))
		}
		d.serializer = s.Serializer
		s.dispatchers = append(s.dispatchers, d)
		s.Dispatcher = d
	}
	return s
}

func (s *TCPServer) SetHub(hub NetHub) *TCPServer {
	s.hub = hub
	return s
}

func (s *TCPServer) SetReadIntercepter(intercepter Intercepter) *TCPServer {
	s.readIntercepter = intercepter
	return s
}

func (s *TCPServer) SetWriteIntercepter(intercepter Intercepter) *TCPServer {
	s.writeIntercepter = intercepter
	return s
}

func (s *TCPServer) EnableMonitor(monitorLog *log4g.Loggers) *TCPServer {
	s.monitor = true
	s.monitorLog = monitorLog
	return s
}

func (s *TCPServer) Start() *TCPServer {

	if s.Serializer == nil {
		s.Serializer = NewProtobufSerializer()
	}

	if len(s.dispatchers) == 0 {
		s.AddDispatchers(NewNamedDispatcher(s.Name))
	}

	var err error
	s.listener, err = net.Listen("tcp", s.Addr)
	if err != nil {
		log4g.Error(s.Addr)
		panic(err)
	}
	log4g.Info("TCP server listening on %s", s.listener.Addr().String())

	if s.hub == nil {
		s.hub = NewNetHub(false, false)
	}
	s.hub.SetSerializer(s.Serializer)

	for _, d := range s.dispatchers {
		d.serializer = s.Serializer
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
				case <-ticker.C:
					s.hub.Statistics()
					trc, twc, prc, pwc := s.hub.PackCount()
					now := time.Now()
					duration := now.Sub(previousTime)
					previousTime = now
					s.monitorLog.Info("")
					s.monitorLog.Info("*[%s status] goroutine: %d, connection: %d", s.Name, runtime.NumGoroutine(), s.hub.Count())
					s.monitorLog.Info("*[%s message] read: %d, write: %d", s.Name, trc, twc)
					s.monitorLog.Info("*[%s msg/sec] read: %d, write: %d", s.Name, prc*int64(time.Second)/int64(duration), pwc*int64(time.Second)/int64(duration))
					trd, twd, ord, owd := s.hub.DataUsage()
					s.monitorLog.Info("*[%s data usage] read: %s, write: %s", s.Name, gutil.HumanReadableByteCount(trd*int64(time.Second)/int64(duration), true), gutil.HumanReadableByteCount(twd*int64(time.Second)/int64(duration), true))
					s.monitorLog.Info("*[%s data/sec] read: %s, write: %s", s.Name, gutil.HumanReadableByteCount(ord*int64(time.Second)/int64(duration), true), gutil.HumanReadableByteCount(owd*int64(time.Second)/int64(duration), true))
				}
			}
		}()
	}

	return s
}

func (s *TCPServer) listen() {

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
				delay = gutil.SmartSleep(delay, maxDelay)
				continue
			}
			break
		}
		log4g.Info("accept connection from %s", netconn.RemoteAddr().String())
		//new event
		conn := newTcpConn(netconn, s.readIntercepter, s.writeIntercepter)
		s.hub.Add(conn.RemoteAddr().String(), conn)
		agent := newNetAgent(s.hub, conn, nil, nil, nil, s.Serializer)
		for _, d := range s.dispatchers {
			d.dispatchConnectionCreatedEvent(agent)
		}

		go func() { // one connection, one goroutine to read
			s.closeConn.Add(1)
			defer s.closeConn.Done()
			newNetReader(conn, s.Serializer, s.dispatchers, s.hub).Read(func(data []byte) bool {
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
				d.dispatchConnectionClosedEvent(agent)
			}
			log4g.Info("disconnected from %s", conn.RemoteAddr().String())
		}()
	}
}

func (s *TCPServer) Close() {

	s.closed = true

	//close listener
	s.closeListen.Add(1)
	s.listener.Close()
	s.closeListen.Wait()
	log4g.Info("closed server[%s] listener", s.Name)

	//close all connections
	//s.hub.CloseConnections()
	//s.closeConn.Wait()
	//log4g.Info("closed server[%s] connections/readers", s.Name)

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

func (s *TCPServer) Wait(others ...Closer) {
	sig := make(chan os.Signal, 1)

	signal.Notify(sig, os.Interrupt, os.Kill, Terminal)
	log4g.Info("server[%s] is closing with signal %v", s.Name, <-sig)

	for _, other := range others {
		other.Close()
	}
	s.Close()
	close(sig)
}
