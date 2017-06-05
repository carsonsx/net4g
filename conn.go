package net4g

import (
	"errors"
	"github.com/carsonsx/log4g"
	"github.com/carsonsx/net4g/util"
	"io"
	"net"
	"time"
)

type NetWriter interface {
	Write(p []byte)
}

type NetConn interface {
	RemoteAddr() net.Addr
	Read() (p []byte, err error)
	Write(p []byte) error
	Close()
	Session() NetSession
	FailedWriteData() [][]byte
}

func NewNetConn(conn net.Conn) NetConn {
	if conn.RemoteAddr().Network() == "tcp" {
		return newTcpConn(conn)
	} else {
		panic("invalid connection name")
	}
}

func newTcpConn(conn net.Conn) *tcpNetConn {
	tcp := new(tcpNetConn)
	tcp.conn = conn
	tcp.writeChan = make(chan []byte, 1000)
	tcp.writeFailedChan = make(chan []byte, 10000)
	tcp.session = NewNetSession()
	tcp.startWriting()
	return tcp
}

type tcpNetConn struct {
	conn       net.Conn
	writeChan  chan []byte
	writeFailedChan  chan []byte
	readChan   chan []byte
	closed     bool
	session    NetSession
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
	}
	return
}

//one connection, one writer, goroutine safe
func (c *tcpNetConn) startWriting() {
	go func() {
		for !c.closed {
			data := <- c.writeChan
			pack := util.AddIntHeader(data, NetConfig.MessageLengthSize, uint64(len(data)), NetConfig.LittleEndian)
			_, err := c.conn.Write(pack)
			if err != nil {
				log4g.Error(err)
				c.writeFailedChan <- data
			} else {
				if log4g.IsTraceEnabled() {
					log4g.Trace("written: %v", data)
				}
			}
		}
	}()
}

func (c *tcpNetConn) Write(p []byte) error {
	if c.closed {
		text := "write to closed network connection"
		log4g.Error(text)
		c.writeFailedChan <- p
		return errors.New(text)
	} else {
		c.writeChan <- p
	}
	return nil
}


func (c *tcpNetConn) FailedWriteData() [][]byte {
	var data [][]byte
	loop:
	for {
		select {
		case d := <- c.writeFailedChan:
			data = append(data, d)
		default:
			break loop
		}
	}
	return data
}

func (c *tcpNetConn) Close() {
	if c.closed {
		return
	}
	c.closed = true
	for len(c.writeChan) > 0 {
		time.Sleep(100)
	}
	//close(c.writeChan)
	err := c.conn.Close()
	if err != nil {
		log4g.Error(err)
	}
}

func (c *tcpNetConn) Session() NetSession {
	return c.session
}
