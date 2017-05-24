package net4g

import (
	"log"
	"net"
	"sync"
	"time"
	"os"
	"os/signal"
	"syscall"
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
	wg              sync.WaitGroup
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

	log.Printf("connected to %s\n", netconn.RemoteAddr().String())
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
		c.wg.Add(1)
		go func() {
			for {
				<-timer.C
				if c.conn.Write(c.heartbeatData) != nil {
					break
				}
			}
			c.wg.Done()
			if !c.closingBySignal {
				c.sig <- Terminal
			}
		}()
	}

	c.wg.Add(1)
	go func() {
		newNetReader(c.conn, c.serializer, c.dispatchers, c.mgr).Read(nil, nil)
		c.mgr.Remove(c.conn)
		for _, d := range c.dispatchers {
			d.CloseSession(c.conn.Session())
		}
		c.wg.Done()
	}()
}

func (c *tcpClient) Write(v interface{}) error {
	var res netRes
	res.conn = c.conn
	res.serializer = c.serializer
	return res.Write(v)
}

func (c *tcpClient) Close() {
	log.Printf("client[%s] listener was closed", c.Addr)
	//close all connections
	c.mgr.CloseConnections()
	c.wg.Wait()
	log.Printf("client[%s] manager was closed", c.Addr)
	c.mgr.Close()
	for _, d := range c.dispatchers {
		d.Destroy()
	}
	log.Printf("client[%s] closed", c.Addr)

}

func (c *tcpClient) Wait()  {
	signal.Notify(c.sig, os.Interrupt, os.Kill)
	log.Printf("client[%s] is closing with signal %v\n", c.Addr, <-c.sig)
	c.closingBySignal = true
	c.Close()

}
