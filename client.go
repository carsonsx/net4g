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

func AddrFn(_addr string) func() (addr string, err error) {
	return func() (addr string, err error) {
		return _addr, nil
	}
}

func NewTcpClient(addrFn func() (addr string, err error)) *TCPClient {
	client := new(TCPClient)
	client.addrFn = addrFn
	client.serializer = GlobalSerializer
	client.dispatchers = append(client.dispatchers, GlobalDispatcher)
	client.AutoReconnect = true
	client.reconnectDelay = reconnect_delay_min
	return client
}

type TCPClient struct {
	addrFn         func() (addr string, err error)
	Addr           string
	RemoteAddr     net.Addr
	conn           NetConn
	AutoReconnect  bool
	reconnectDelay int
	serializer     Serializer
	dispatchers    []*dispatcher
	mgr            *NetManager
	heartbeat      bool
	heartbeatData  []byte
	sig            chan os.Signal
	tryClose       bool
	closeConn      sync.WaitGroup
	connected      bool
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
	conn := NewNetConn(netconn)
	c.RemoteAddr = conn.RemoteAddr()
	c.mgr.Add(conn)
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
	return nil
}

func (c *TCPClient) Run() *TCPClient {


	c.sig = make(chan os.Signal, 1)

	// Init the connection manager
	c.mgr = new(NetManager)
	c.mgr.heartbeat = c.heartbeat
	c.mgr.Start()

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
						log4g.Warn(c.tryClose)
						log4g.Warn(c.AutoReconnect)
						if c.tryClose || !c.AutoReconnect {
							log4g.Info("end heartbeat")
							break
						}
					}
				}
			}
			c.closeConn.Done()
			if !c.tryClose {
				c.sig <- Terminal
			}
		}()
	}

	c.closeConn.Add(1)

	go func() {

		for {

			if c.connect() == nil {
				newNetReader(c.conn, c.serializer, c.dispatchers, c.mgr).Read(func(data []byte) bool {
					if IsHeartbeatData(data) {
						log4g.Trace("heartbeat from server")
						c.mgr.Heartbeat(c.conn)
						return false
					}
					return true
				})
				c.mgr.Remove(c.conn)
				c.connected = false
				for _, d := range c.dispatchers {
					d.handleConnectionClosed(c.conn.Session())
				}
			}

			if !c.tryClose && c.AutoReconnect {
				log4g.Info("delay %d millisecond to AutoReconnect", c.reconnectDelay)
				time.Sleep(time.Duration(c.reconnectDelay) * time.Millisecond)
				c.reconnectDelay *= 2
				if c.reconnectDelay > reconnect_delay_max {
					c.reconnectDelay = reconnect_delay_max
				}
			} else {
				log4g.Info("disconnected")
				break
			}
		}

		c.closeConn.Done()
	}()

	return c
}

func (c *TCPClient) Write(v interface{}) error {
	var res netRes
	res.conn = c.conn
	res.serializer = c.serializer
	return res.Write(v)
}

func (c *TCPClient) Close() {

	c.tryClose = true

	//close all connections
	c.mgr.CloseConnections()
	c.closeConn.Wait()
	log4g.Info("closed client[%s] connections/readers", c.Addr)

	//close net manager
	c.mgr.Destroy()
	log4g.Info("closed client[%s] manager", c.Addr)

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
