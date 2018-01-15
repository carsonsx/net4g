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

func NewNetAddrFn(addrs ...string) func() (addrs []string, err error) {
	return func() (addrSlice []string, err error) {
		for _, addr := range addrs {
			addrSlice = append(addrSlice, addr)
		}
		return
	}
}

func NewTcpClient(name string, addrFn func() (addrs []string, err error)) *TCPClient {
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
	addrFn           func() (addrs []string, err error)
	readIntercepter  Intercepter
	writeIntercepter Intercepter
	AutoReconnect    bool
	reconnectDelay   int
	serializer       Serializer
	dispatchers      []*Dispatcher
	hub              NetHub
	heartbeatData    []byte
	sig              chan os.Signal
	closed           bool
	firstConnected   sync.WaitGroup
	monitor          bool
	monitorLog       *log4g.Logger
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

func (c *TCPClient) EnableMonitor(monitorLog *log4g.Logger) *TCPClient {
	c.monitor = true
	c.monitorLog = monitorLog
	if c.monitorLog == nil {
		c.monitorLog = log4g.NewLogger()
	}
	return c
}

func (c *TCPClient) DisableAutoReconnect() *TCPClient {
	c.AutoReconnect = false
	return c
}

func (c *TCPClient) Connect() *TCPClient {

	if c.hub == nil {
		c.hub = NewNetHub(HEART_BEAT_MODE_NONE, true)
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
					c.monitorLog.Info("*[%s msg/sec] read: %d, write: %d", c.name, gutil.ToPerSecond(prc, duration), gutil.ToPerSecond(pwc, duration))
					trd, twd, ord, owd := c.hub.DataUsage()
					c.monitorLog.Info("*[%s data usage] read: %s, write: %s", c.name, gutil.HumanReadableByteCount(gutil.ToPerSecond(trd, duration), true), gutil.HumanReadableByteCount(gutil.ToPerSecond(twd, duration), true))
					c.monitorLog.Info("*[%s data/sec] read: %s, write: %s", c.name, gutil.HumanReadableByteCount(gutil.ToPerSecond(ord, duration), true), gutil.HumanReadableByteCount(gutil.ToPerSecond(owd, duration), true))
				}
			}
		}()
	}

	return c
}

func (c *TCPClient) doConnect(addr string) {

	var connected bool

	for {
		conn, err := c.connect(addr)
		if err == nil {
			if !connected {
				c.firstConnected.Done()
				connected = true
			}
			newNetReader(conn, c.serializer, nil, c.dispatchers, c.hub).Read(func(data []byte) bool {
				if IsHeartbeatData(data) {
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
		} else {
			log4g.Error(err)
		}

		if c.closed || !c.AutoReconnect {
			log4g.Info("disconnected")
			log4g.Error(err)
			c.sig <- Terminal
			if !connected {
				c.firstConnected.Done()
			}
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

func (c *TCPClient) connect(addr string) (conn NetConn, err error) {
	var netconn net.Conn
	netconn, err = net.Dial("tcp", addr)
	if err != nil {
		log4g.Error(err)
		return
	}
	log4g.Info("connected to %s from %s", netconn.RemoteAddr().String(), netconn.LocalAddr().String())
	c.reconnectDelay = reconnect_delay_min
	conn = newTcpConn(netconn, c.readIntercepter, c.writeIntercepter)
	//_conn := c.hub.Get(name)
	//if _conn != nil {
	//	failedData := _conn.NotWrittenData()
	//	if len(failedData) > 0 {
	//		log4g.Info("found %d failed write data, will rewrite by new connection", len(failedData))
	//	}
	//	for _, data := range failedData {
	//		conn.Write(data)
	//	}
	//}
	c.hub.Add(conn.LocalAddr().String(), conn)
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
	c.hub.CloseAllConnections()
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
	log4g.Flush()
}
