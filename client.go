package net4g

import (
	"github.com/carsonsx/log4g"
	"net"
	"os"
	"os/signal"
	"sync"
	"time"
)

const (
	reconnect_delay_min = 100
	reconnect_delay_max = 10000
)

func NewTcpClient(addrFn func() (addrs []*NetAddr, err error)) *TCPClient {
	client := new(TCPClient)
	client.addrFn = addrFn
	client.AutoReconnect = true
	client.reconnectDelay = reconnect_delay_min
	client.sig = make(chan os.Signal, 2)
	return client
}

type TCPClient struct {
	addrFn         func() (addrs []*NetAddr, err error)
	AutoReconnect  bool
	reconnectDelay int
	serializer     Serializer
	dispatchers    []*dispatcher
	hub            NetHub
	heartbeat      bool
	heartbeatData  []byte
	sig            chan os.Signal
	closed         bool
	firstConnected sync.WaitGroup
}

func (c *TCPClient) SetSerializer(serializer Serializer) *TCPClient {
	c.serializer = serializer
	return c
}

func (c *TCPClient) AddDispatchers(dispatchers ...*dispatcher) *TCPClient {
	for _, d := range dispatchers {
		c.dispatchers = append(c.dispatchers, d)
	}
	return c
}

func (c *TCPClient) EnableHeartbeat() *TCPClient {
	c.heartbeat = true
	return c
}

func (c *TCPClient) DisableAutoReconnect() *TCPClient {
	c.AutoReconnect = false
	return c
}

func (c *TCPClient) Start() *TCPClient {

	// Init the connection manager
	c.hub = NewNetHub(c.heartbeat, true)

	for _, d := range c.dispatchers {
		d.serializer = c.serializer
		d.hub = c.hub
	}

	addrs, err := c.addrFn()
	if err != nil {
		panic(err)
	}

	c.firstConnected.Add(len(addrs))

	for _, addr := range addrs {
		go c.doConnect(addr)
	}

	// for first connection
	c.firstConnected.Wait()

	if c.heartbeat {
		timer := time.NewTicker(NetConfig.HeartbeatFrequency)
		if c.heartbeatData == nil {
			c.heartbeatData = []byte{}
		}
		go func() {
			for {
				if c.hub.Closed() {
					break
				}
				c.hub.BroadcastAll(c.heartbeatData)
				<-timer.C
			}
		}()
	}

	return c
}


func (c *TCPClient) doConnect(addr *NetAddr) {

	var connected bool

	for {
		if conn, err := c.connect(addr.Key, addr.Addr); err == nil {
			if !connected {
				c.firstConnected.Done()
				connected = true
			}
			newNetReader(conn, c.serializer, c.dispatchers, c.hub).Read(func(data []byte) bool {
				if IsHeartbeatData(data) {
					log4g.Trace("heartbeat from server")
					c.hub.Heartbeat(conn)
					return false
				}
				return true
			})
			c.hub.Remove(conn)
			agent := newNetAgent(c.hub, conn, nil, nil, c.serializer)
			for _, d := range c.dispatchers {
				d.handleConnectionClosed(agent)
			}
		}

		if c.closed || !c.AutoReconnect {
			log4g.Info("disconnected")
			break
		}

		log4g.Info("delay %d millisecond to reconnect", c.reconnectDelay)
		time.Sleep(time.Duration(c.reconnectDelay) * time.Millisecond)
		c.reconnectDelay *= 2
		if c.reconnectDelay > reconnect_delay_max {
			c.reconnectDelay = reconnect_delay_max
		}
	}

}

func (c *TCPClient) connect(name, addr string) (conn NetConn, err error) {
	var netconn net.Conn
	netconn, err = net.Dial("tcp", addr)
	if err != nil {
		log4g.Error(err)
		return
	}
	log4g.Info("connected to %s", netconn.RemoteAddr().String())
	c.reconnectDelay = reconnect_delay_min
	conn = newTcpConn(netconn)
	_conn := c.hub.Get(name)
	if _conn != nil {
		failedData := _conn.NotWrittenData()
		if len(failedData) > 0 {
			log4g.Info("found %d failed write data, will rewrite by new connection", len(failedData))
		}
		for _, data := range failedData {
			conn.Write(data)
		}
	}
	c.hub.Add(name, conn)
	agent := newNetAgent(c.hub, conn, nil, nil, c.serializer)
	for _, d := range c.dispatchers {
		d.handleConnectionCreated(agent)
	}
	return
}

func (c *TCPClient) Close() {

	c.closed = true

	//close all connections
	c.hub.CloseConnections()
	log4g.Info("closed client connections")

	//close net hub
	c.hub.Destroy()
	log4g.Info("closed client hub")

	//close dispatchers
	for _, d := range c.dispatchers {
		d.Destroy()
	}
	log4g.Info("closed client dispatcher")

	log4g.Info("closed client")

}

func (c *TCPClient) Wait() {
	signal.Notify(c.sig, os.Interrupt, os.Kill, Terminal)
	log4g.Info("client is closing with signal %v\n", <-c.sig)
	c.Close()
}
