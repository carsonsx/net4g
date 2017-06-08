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
	return client
}

type TCPClient struct {
	addrFn           func() (addrs []*NetAddr, err error)
	Addr             string
	AutoReconnect    bool
	reconnectDelay   int
	serializer       Serializer
	dispatchers      []*dispatcher
	hub              *NetHub
	heartbeat        bool
	heartbeatData    []byte
	sig              chan os.Signal
	closed           bool
	closeConn        sync.WaitGroup
	connected        bool
	connectedHandler func (conn NetConn, client *TCPClient)
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

func (c *TCPClient) OnConnected(h func (conn NetConn, client *TCPClient)) *TCPClient {
	c.connectedHandler = h
	return c
}

func (c *TCPClient) connect() error {
	var err error
	c.Addr, err = c.addrFn()
	if err != nil {
		return err
	}
	netconn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		log4g.Error(err)
		return err
	}
	log4g.Info("connected to %s", netconn.RemoteAddr().String())
	c.connected = true
	c.reconnectDelay = reconnect_delay_min
	conn := newTcpConn(netconn)
	c.hub.Add(conn)
	if c.conn != nil {
		failedData := c.conn.NotWrittenData()
		if len(failedData) > 0 {
			log4g.Info("found %d failed write data, will rewrite by new connection", len(failedData))
		}
		for _, data := range failedData {
			conn.Write(data)
		}
	}
	c.conn = conn

	if c.connectedHandler != nil {
		c.connectedHandler(conn, c)
	}

	return nil
}

func (c *TCPClient) Start() *TCPClient {

	if c.serializer == nil {
		c.serializer = NewJsonSerializer()
	}

	c.sig = make(chan os.Signal, 1)

	// Init the connection manager
	c.hub = new(NetHub)
	c.hub.heartbeat = c.heartbeat
	c.hub.Start()

	if c.heartbeat {
		timer := time.NewTicker(NetConfig.HeartbeatFrequency)
		if c.heartbeatData == nil {
			c.heartbeatData = []byte{}
		}
		c.closeConn.Add(1)
		go func() {
			for {
				<-timer.C
				if c.connected {
					if c.conn.Write(c.heartbeatData) != nil {
						log4g.Warn(c.closed)
						log4g.Warn(c.AutoReconnect)
						if c.closed || !c.AutoReconnect {
							log4g.Info("end heartbeat")
							break
						}
					}
				}
			}
			c.closeConn.Done()
			if !c.closed {
				c.sig <- Terminal
			}
		}()
	}

	c.closeConn.Add(1)

	var connectedWG sync.WaitGroup

	connectedWG.Add(1)

	go func() {

		for {

			if c.connect() == nil {
				connectedWG.Done()
				newNetReader(c.conn, c.serializer, c.dispatchers, c.hub).Read(func(data []byte) bool {
					if IsHeartbeatData(data) {
						log4g.Trace("heartbeat from server")
						c.hub.Heartbeat(c.conn)
						return false
					}
					return true
				})
				c.hub.Remove(c.conn)
				c.connected = false
				for _, d := range c.dispatchers {
					d.handleConnectionClosed(c.conn.Session())
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

			connectedWG.Add(1)
		}

		c.closeConn.Done()

		if !c.closed {
			c.sig <- Terminal
		}
	}()

	// for first connection
	connectedWG.Wait()

	return c
}

func (c *TCPClient) Write(v interface{}) error {
	var a netAgent
	a.conn = c.conn
	a.serializer = c.serializer
	return a.Write(v)
}

func (c *TCPClient) Close() {

	c.closed = true

	//close all connections
	c.hub.CloseConnections()
	c.closeConn.Wait()
	log4g.Info("closed client[%s] connections", c.Addr)

	//close net hub
	c.hub.Destroy()
	log4g.Info("closed client[%s] hub", c.Addr)

	//close dispatchers
	for _, d := range c.dispatchers {
		d.Destroy()
	}
	log4g.Info("closed client[%s] dispatcher", c.Addr)

	log4g.Info("closed client[%s]", c.Addr)

}

func (c *TCPClient) Wait() {
	signal.Notify(c.sig, os.Interrupt, os.Kill, Terminal)
	log4g.Info("client[%s] is closing with signal %v\n", c.Addr, <-c.sig)
	c.Close()

}
