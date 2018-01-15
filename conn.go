package net4g

import (
	"errors"
	"github.com/carsonsx/log4g"
	"io"
	"net"
	"sync"
	"time"
	"bytes"
	"container/list"
)

type Handler interface {

}

type Pipeline interface {
	AddLast(handler Handler)
	AddFist(handler Handler)
}

type pipeline struct {
	handlers list.List
}

func (p *pipeline) AddFirst(handler Handler)  {
	p.handlers.PushFront(handler)
}

func (p *pipeline) AddLast(handler Handler)  {
	p.handlers.PushBack(handler)
}

func (p *pipeline) Slice() []Handler {
	var handlers []Handler
	for e:=p.handlers.Front() ; e !=nil ; e=e.Next() {
		handlers = append(handlers, e.Value)
	}
	return handlers
}

type NetConn interface {
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	//Pipeline() Pipeline
	Read() (p []byte, err error)
	Write(p []byte) error
	Session() NetSession
	Close()
	IsClosed() bool
	NotWrittenData() [][]byte
	PopCount() (read int64, write int64)
	PopDataUsage() (read int64, write int64)
}

func newTcpConn(conn net.Conn, readIntercepter, writeIntercepter Intercepter) *tcpNetConn {
	tcp := new(tcpNetConn)
	tcp.conn = conn
	tcp.readIntercepter = readIntercepter
	tcp.writeIntercepter = writeIntercepter
	tcp.writeChan = make(chan []byte, NetConfig.WriteChanSize)
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
	conn             net.Conn
	session          NetSession
	writeChan        chan []byte
	readIntercepter  Intercepter
	writeIntercepter Intercepter
	closeChan        chan bool
	closing          bool
	closed           bool
	closeMutex       sync.Mutex
	closeWG          sync.WaitGroup
	buffer           []byte
	offset           int
	readCount        int64
	writtenCount     int64
	readDataUsage    int64
	writeDataUsage   int64
}

func (c *tcpNetConn) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

func (c *tcpNetConn) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

func (c *tcpNetConn) Read() (data []byte, err error) {

	if NetConfig.ReadMode == READ_MODE_BY_LENGTH {

		header := make([]byte, NetConfig.LengthSize)
		//log4g.Trace("try to read data length")
		_, err = io.ReadFull(c.conn, header)
		if err != nil {
			if err != io.EOF && !c.closing {
				log4g.Error(err)
			}
			return
		}
		msgLen := GetIntHeader(header[NetConfig.LengthIndex:], NetConfig.LengthSize, NetConfig.LittleEndian)
		data = make([]byte, msgLen)
		if msgLen > 0 {
			log4g.Trace("try to read %d data", msgLen)
			_, err = io.ReadFull(c.conn, data)
			if err != nil {
				var buf bytes.Buffer
				_, err2 := io.Copy(&buf, c.conn)
				if err2 != nil {
					log4g.Error(buf)
				}
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
				if err != io.EOF && !c.closing {
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
		log4g.Trace("[%d]read raw msg: %v", len(data), data)
		//log4g.Trace("read: %v", string(data))
	}

	c.readCount++
	//log4g.Info("read countMsg %d %s", c.readCount, c.RemoteAddr())
	c.readDataUsage += int64(len(data))

	//intercept data
	if c.readIntercepter != nil {
		data = c.readIntercepter.Intercept(data)
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
			case writingData := <-c.writeChan:
				data := writingData
				//intercept data
				if c.writeIntercepter != nil {
					data = c.writeIntercepter.Intercept(data)
				}
				var pack []byte
				if NetConfig.ReadMode == READ_MODE_BY_LENGTH {
					pack = AddIntHeader(data, NetConfig.LengthSize, uint64(len(data)), NetConfig.LittleEndian)
				} else if NetConfig.ReadMode == READ_MODE_BEGIN_END {
					newData := append(data, NetConfig.EndBytes...)
					pack = make([]byte, len(NetConfig.BeginBytes)+len(newData))
					copy(pack, NetConfig.BeginBytes)
					copy(pack[len(NetConfig.BeginBytes):], newData)
				} else {
					panic("Wrong read mode")
				}
				log4g.Trace("[%d]write pack msg: %v", len(pack), pack)
				_, err := c.conn.Write(pack)
				if err != nil {
					log4g.Error(err)
					if NetConfig.KeepWriteData {
						c.writeChan <- writingData
					}
					break outer
				} else {
					c.session.Set(SESSION_CONNECT_LAST_WRITE_TIME, time.Now())
					c.writtenCount++
					//log4g.Info("written countMsg %d %s", c.writtenCount, c.RemoteAddr())
					c.writeDataUsage += int64(len(pack))
				}
			}
		}
		c.closed = true
	}()
}

func (c *tcpNetConn) Write(p []byte) error {
	var err error
	if c.closed {
		text := "write to closed network connection"
		log4g.Info(text)
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
	c.closing = true
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
	log4g.Info("closed connection %s", c.RemoteAddr().String())
}

func (c *tcpNetConn) IsClosed() bool {
	return c.closed
}

func (c *tcpNetConn) Session() NetSession {
	return c.session
}

func (c *tcpNetConn) PopCount() (read int64, write int64) {
	read = c.readCount
	c.readCount = 0
	write = c.writtenCount
	c.writtenCount = 0
	return
}

func (c *tcpNetConn) PopDataUsage() (read int64, write int64) {
	read = c.readDataUsage
	c.readDataUsage = 0
	write = c.writeDataUsage
	c.writeDataUsage = 0
	return
}
