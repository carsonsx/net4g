package net4g

import (
	"errors"
	"github.com/carsonsx/log4g"
	"github.com/carsonsx/net4g/util"
	"io"
	"net"
	"sync"
	"time"
)

type NetWriter interface {
	Write(p []byte)
}

type NetConn interface {
	RemoteAddr() net.Addr
	Read() (p []byte, err error)
	Write(p []byte) error
	Session() NetSession
	Close()
	NotWrittenData() [][]byte
}

func newTcpConn(conn net.Conn) *tcpNetConn {
	tcp := new(tcpNetConn)
	tcp.conn = conn
	tcp.writeChan = make(chan []byte, 10000)
	tcp.closeChan = make(chan bool)
	tcp.session = NewNetSession()
	tcp.session.Set(SESSION_CONNECT_ESTABLISH_TIME, time.Now())
	tcp.startWriting()
	return tcp
}

type tcpNetConn struct {
	conn            net.Conn
	session         NetSession
	writeChan       chan []byte
	readChan        chan []byte
	closeChan       chan bool
	closed          bool
	closeMutex      sync.Mutex
	closeWG         sync.WaitGroup
}

func (c *tcpNetConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *tcpNetConn) Read() (data []byte, err error) {
	header := make([]byte, NetConfig.MessageLengthSize)
	_, err = io.ReadFull(c.conn, header)
	if err != nil {
		if err != io.EOF {
			log4g.Error(err)
		}
		return
	}
	msgLen := util.GetIntHeader(header, NetConfig.MessageLengthSize, NetConfig.LittleEndian)
	data = make([]byte, msgLen)
	if msgLen > 0 {
		_, err = io.ReadFull(c.conn, data)
		if err != nil {
			log4g.Error(err)
			return
		}
	}
	if log4g.IsTraceEnabled() {
		log4g.Trace("read: %v", data)
		//log4g.Trace("read: %v", string(data))
	}
	return
}

//one connection, one writer, goroutine safe
func (c *tcpNetConn) startWriting() {
	go func() {
		c.closeWG.Add(1)
		defer c.closeWG.Done()
	outer:
		for {
			select {
			case <-c.closeChan:
				break outer
			case data := <-c.writeChan:
				pack := util.AddIntHeader(data, NetConfig.MessageLengthSize, uint64(len(data)), NetConfig.LittleEndian)
				_, err := c.conn.Write(pack)
				if err != nil {
					log4g.Error(err)
					if NetConfig.KeepWriteData {
						c.writeChan <- data
					}
					break outer
				} else {
					c.session.Set(SESSION_CONNECT_LAST_WRITE_TIME, time.Now())
					if log4g.IsTraceEnabled() {
						log4g.Trace("written: %v", data)
					}
				}
			}
		}
	}()
}

func (c *tcpNetConn) Write(p []byte) error {
	var err error
	if c.closed {
		text := "write to closed network connection"
		log4g.Error(text)
		err = errors.New(text)
		if NetConfig.KeepWriteData {
			c.writeChan <- p
		}
	} else {
		c.writeChan <- p
	}

	return err
}

func (c *tcpNetConn) NotWrittenData() [][]byte {
	var data [][]byte
outer:
	for {
		select {
		case d := <-c.writeChan:
			data = append(data, d)
		default:
			break outer
		}
	}
	return data
}

func (c *tcpNetConn) Close() {
	log4g.Debug("closing connection %s", c.RemoteAddr().String())
	c.closeMutex.Lock()
	defer c.closeMutex.Unlock()
	if c.closed {
		return
	}
	close(c.closeChan)
	c.closeWG.Wait()
	err := c.conn.Close()
	if err != nil {
		log4g.Error(err)
	}
	c.closed = true
	log4g.Debug("closed connection %s", c.RemoteAddr().String())
}

func (c *tcpNetConn) Session() NetSession {
	return c.session
}
