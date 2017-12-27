package net4g

import (
	"github.com/carsonsx/log4g"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"time"
	"github.com/carsonsx/gutil"
)

const (
	reconnect_delay_min = 100
	reconnect_delay_max = 10000
)

func NewTcpClient(name string, addrFn func() (addrs []*NetAddr, err error)) *TCPClient {
	client := new(TCPClient)
	client.name = name
	client.addrFn = addrFn
	client.AutoReconnect = true
	client.reconnectDelay = reconnect_delay_min
	client.sig = make(chan os.Signal, 2)
	return client
}

type TCPClient struct {
	name             string
	addrFn           func() (addrs []*NetAddr, err error)
	readIntercepter  Intercepter
	writeIntercepter Intercepter
	AutoReconnect    bool
	reconnectDelay   int
	serializer       Serializer
	dispatchers      []*Dispatcher
	hub              NetHub
	heartbeat        bool
	heartbeatData    []byte
	sig              chan os.Signal
	closed           bool
	firstConnected   sync.WaitGroup
	monitor          bool
	monitorLog       *log4g.Loggers
	startTime        time.Time
}

func (c *TCPClient) SetSerializer(serializer Serializer) *TCPClient {
	c.serializer = serializer
	return c
}

func (c *TCPClient) AddDispatchers(dispatchers ...*Dispatcher) *TCPClient {
	for _, d := range dispatchers {
		c.dispatchers = append(c.dispatchers, d)
	}
	return c
}

func (c *TCPClient) SetReadIntercepter(intercepter Intercepter) *TCPClient {
	c.readIntercepter = intercepter
	return c
}

func (c *TCPClient) SetWriteIntercepter(intercepter Intercepter) *TCPClient {
	c.writeIntercepter = intercepter
	return c
}
func (c *TCPClient) SetHub(hub NetHub) *TCPClient {
	c.hub = hub
	return c
}

func (c *TCPClient) EnableMonitor(monitorLog *log4g.Loggers) *TCPClient {
	c.monitor = true
	c.monitorLog = monitorLog
	if c.monitorLog == nil {
		c.monitorLog = log4g.NewLoggers()
	}
	return c
}

func (c *TCPClient) DisableAutoReconnect() *TCPClient {
	c.AutoReconnect = false
	return c
}

func (c *TCPClient) Connect() *TCPClient {

	if c.hub == nil {
		c.hub = NewNetHub(false, true)
	}
	c.hub.SetSerializer(c.serializer)

	for _, d := range c.dispatchers {
		d.serializer = c.serializer
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
		timer := time.NewTicker(NetConfig.HeartbeatFrequency * time.Second)
		c.heartbeatData = NetConfig.HeartbeatData
		go func() {
			for {
				if c.hub.Closed() {
					break
				}
				log4g.Trace("[client] heart beat...")
				c.hub.BroadcastAll(c.heartbeatData)
				<-timer.C
			}
		}()
	}

	c.startTime = time.Now()

	if c.monitor {
		go func() {
			ticker := time.NewTicker(time.Duration(NetConfig.MonitorBeat) * time.Second)
			previousTime := c.startTime
			for {
				if c.closed {
					ticker.Stop()
					break
				}
				select {
				case <-ticker.C:
					c.hub.Statistics()
					trc, twc, prc, pwc := c.hub.PackCount()
					now := time.Now()
					duration := now.Sub(previousTime)
					previousTime = now
					c.monitorLog.Info("")
					c.monitorLog.Info("*[%s status] goroutine: %d, connection: %d", c.name, runtime.NumGoroutine(), c.hub.Count())
					c.monitorLog.Info("*[%s message] read: %d, write: %d", c.name, trc, twc)
					c.monitorLog.Info("*[%s msg/sec] read: %d, write: %d", c.name, prc*int64(time.Second)/int64(duration), pwc*int64(time.Second)/int64(duration))
					trd, twd, ord, owd := c.hub.DataUsage()
					c.monitorLog.Info("*[%s data usage] read: %s, write: %s", c.name, gutil.HumanReadableByteCount(trd*int64(time.Second)/int64(duration), true), gutil.HumanReadableByteCount(twd*int64(time.Second)/int64(duration), true))
					//s.monitorLog.Info("*[%s data  offset] read: %s, write: %s", s.Name, gutil.HumanReadableByteCount(ord, true), gutil.HumanReadableByteCount(owd, true))
					c.monitorLog.Info("*[%s data/sec] read: %s, write: %s", c.name, gutil.HumanReadableByteCount(ord*int64(time.Second)/int64(duration), true), gutil.HumanReadableByteCount(owd*int64(time.Second)/int64(duration), true))
				}
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
			agent := newNetAgent(c.hub, conn, nil, nil, nil, c.serializer)
			for _, d := range c.dispatchers {
				d.dispatchConnectionClosedEvent(agent)
			}
		}

		if c.closed || !c.AutoReconnect {
			log4g.Info("disconnected")
			c.sig <- Terminal
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
	conn = newTcpConn(netconn, c.readIntercepter, c.writeIntercepter)
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
	agent := newNetAgent(c.hub, conn, nil, nil, nil, c.serializer)
	for _, d := range c.dispatchers {
		d.dispatchConnectionCreatedEvent(agent)
	}
	return
}

func (c *TCPClient) Close() {

	c.closed = true

	//close all connections
	//c.hub.CloseConnections()
	//log4g.Info("closed client[%s] connections", c.name)

	//close net hub
	c.hub.Destroy()
	log4g.Info("closed client[%s] hub", c.name)

	//close dispatchers
	for _, d := range c.dispatchers {
		d.Destroy()
	}
	log4g.Info("closed client[%s] dispatcher", c.name)

	log4g.Info("closed client[%s]", c.name)

}

func (c *TCPClient) Wait() {
	signal.Notify(c.sig, os.Interrupt, os.Kill, Terminal)
	log4g.Info("client[%s] is closing with signal %v\n", c.name, <-c.sig)
	c.Close()
}
