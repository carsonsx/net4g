package net4g

import (
	"log"
	"net"
	"sync"
	"time"
	"os"
	"os/signal"
)

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
	c.mgr.Run()
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
				c.sig <- os.Interrupt
			}
		}()
	}

	c.wg.Add(1)
	go func() {
		newNetReader(c.conn, c.serializer, c.dispatchers, c.mgr).Read(nil, nil)
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
	c.conn.Close()
}

func (c *tcpClient) SafeWait()  {
	signal.Notify(c.sig, os.Interrupt, os.Kill)
	log.Printf("client is closing with signal %v\n", <-c.sig)
	c.closingBySignal = true
	c.mgr.Close()
	c.wg.Wait()

}
