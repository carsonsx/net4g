package net4g

import (
	"github.com/carsonsx/log4g"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var Terminal = syscall.SIGTERM

func NewTcpClient(addr string, serializer ...Serializer) *tcpClient {
	client := new(tcpClient)
	client.Addr = addr
	if len(serializer) > 0 {
		client.serializer = serializer[0]
	} else {
		client.serializer = NewEmptySerializer()
	}
	return client
}

type tcpClient struct {
	Addr            string
	conn            NetConn
	serializer      Serializer
	dispatchers     []*dispatcher
	mgr             *NetManager
	heartbeat       bool
	heartbeatData   []byte
	sig             chan os.Signal
	closingBySignal bool
	closeConn              sync.WaitGroup
}

func (c *tcpClient) AddDispatchers(dispatchers ...*dispatcher) {
	for _, d := range dispatchers {
		c.dispatchers = append(c.dispatchers, d)
	}
}

func (c *tcpClient) EnableHeartbeat() {
	c.heartbeat = true
}

func (c *tcpClient) Run() {
	netconn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		panic(err)
	}

	c.sig = make(chan os.Signal, 1)

	log4g.Info("connected to %s", netconn.RemoteAddr().String())
	c.conn = NewNetConn(netconn)

	// Init the connection manager
	c.mgr = new(NetManager)
	c.mgr.heartbeat = c.heartbeat
	c.mgr.Start()
	c.mgr.Add(c.conn)

	if c.heartbeat {
		timer := time.NewTicker(NetConfig.HeartbeatFrequency)
		if c.heartbeatData == nil {
			c.heartbeatData = []byte{}
		}
		c.closeConn.Add(1)
		go func() {
			for {
				<-timer.C
				if c.conn.Write(c.heartbeatData) != nil {
					break
				}
			}
			c.closeConn.Done()
			if !c.closingBySignal {
				c.sig <- Terminal
			}
		}()
	}

	c.closeConn.Add(1)
	go func() {
		newNetReader(c.conn, c.serializer, c.dispatchers, c.mgr).Read(nil, func(data []byte) bool {
			if IsHeartbeatData(data) {
				log4g.Trace("heartbeat from server")
				c.mgr.Heartbeat(c.conn)
				return false
			}
			return true
		})
		c.mgr.Remove(c.conn)
		for _, d := range c.dispatchers {
			d.CloseSession(c.conn.Session())
		}
		c.closeConn.Done()
	}()
}

func (c *tcpClient) Write(v interface{}) error {
	var res netRes
	res.conn = c.conn
	res.serializer = c.serializer
	return res.Write(v)
}

func (c *tcpClient) Close() {

	//close all connections
	c.mgr.CloseConnections()
	c.closeConn.Wait()
	log4g.Info("closed client[%s] connections/readers", c.Addr)

	//close net manager
	c.mgr.Close()
	log4g.Info("closed client[%s] manager", c.Addr)

	//close dispatchers
	for _, d := range c.dispatchers {
		d.Close()
	}
	log4g.Info("closed client[%s] dispatcher", c.Addr)

	log4g.Info("closed client[%s]", c.Addr)

}

func (c *tcpClient) Wait() {
	signal.Notify(c.sig, os.Interrupt, os.Kill)
	log4g.Info("client[%s] is closing with signal %v\n", c.Addr, <-c.sig)
	c.closingBySignal = true
	c.Close()

}
