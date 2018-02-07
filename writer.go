package net4g

type NetWriter interface {
	Write(p []byte)
}

func NewNetWriter(conn NetConn) NetWriter {
	writer := new (netWriter)
	writer.conn = conn
	writer.writeChan = make(chan []byte, NetConfig.WriteChanSize)
	writer.doWrite()
	return writer
}

type netWriter struct {
	conn NetConn
	writeChan        chan []byte
}

//one connection, one writer, goroutine safe
func (c *netWriter) Write(data []byte) {
	c.writeChan <- data
}

//one connection, one writer, goroutine safe
func (c *netWriter) doWrite() {
	go func() {
		for {
			select {
			//case <-c.closeChan:
			//	break outer
			case writingData := <-c.writeChan:
				if err := c.conn.Write(writingData); err != nil {
					if NetConfig.CacheWriteFailedData {
						//c.writeChan <- writingData
					}
				}
			}
		}
	}()
}