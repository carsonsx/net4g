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
	if NetConfig.ReadMode == READ_MODE_BEGIN_END {
		tcp.buffer = make([]byte, NetConfig.MaxLength)
	}
	tcp.startWriting()
	return tcp
}

type tcpNetConn struct {
	conn       net.Conn
	session    NetSession
	writeChan  chan []byte
	readChan   chan []byte
	closeChan  chan bool
	closed     bool
	closeMutex sync.Mutex
	closeWG    sync.WaitGroup
	buffer     []byte
	offset     int
}

func (c *tcpNetConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *tcpNetConn) Read() (data []byte, err error) {

	if NetConfig.ReadMode == READ_MODE_BY_LENGTH {

		header := make([]byte, NetConfig.LengthSize)
		_, err = io.ReadFull(c.conn, header)
		if err != nil {
			if err != io.EOF {
				log4g.Error(err)
			}
			return
		}
		msgLen := util.GetIntHeader(header[NetConfig.LengthIndex:], NetConfig.LengthSize, NetConfig.LittleEndian)
		data = make([]byte, msgLen)
		if msgLen > 0 {
			_, err = io.ReadFull(c.conn, data)
			if err != nil {
				log4g.Error(err)
				return
			}
		}
	} else if NetConfig.ReadMode == READ_MODE_BEGIN_END {

		foundBegin := false
		foundEnd := false

		for {
			var n int
			n, err = c.conn.Read(c.buffer[c.offset:])
			if err != nil {
				if err != io.EOF {
					log4g.Error(err)
				}
				return
			}
			c.offset += n
			var begin int
			for i, b := range c.buffer {

				if !foundBegin {
					foundBegin = true
					for j, bb := range NetConfig.BeginBytes {
						if c.buffer[i+j] != bb {
							foundBegin = false
							break
						}
					}
					if foundBegin {
						begin = i + len(NetConfig.BeginBytes)
					}
				}

				if !foundEnd {
					foundEnd = true
					for j, bb := range NetConfig.EndBytes {
						if c.buffer[i+j] != bb {
							foundEnd = false
							break
						}
					}
				}

				if foundEnd {
					c.offset = 0
					return
				}

				if foundBegin && i >= begin && !foundEnd {
					data = append(data, b)
				}
			}
		}

	} else {
		panic("Wrong read mode")
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
				var pack []byte
				if NetConfig.ReadMode == READ_MODE_BY_LENGTH {
					pack = util.AddIntHeader(data, NetConfig.LengthSize, uint64(len(data)), NetConfig.LittleEndian)
				} else if NetConfig.ReadMode == READ_MODE_BEGIN_END {
					newData := append(data, NetConfig.EndBytes...)
					pack = make([]byte, len(NetConfig.BeginBytes)+len(newData))
					copy(pack, NetConfig.BeginBytes)
					copy(pack[len(NetConfig.BeginBytes):], newData)
				} else {
					panic("Wrong read mode")
				}
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
						log4g.Trace("written data: %v", data)
						log4g.Trace("written pack: %v", pack)
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
