package net4g

import (
	"bytes"
	"github.com/carsonsx/log4g"
	"runtime/debug"
	"time"
)

func IsHeartbeatData(data []byte) bool {
	return len(data) == len(NetConfig.HeartbeatData) && bytes.Equal(data, NetConfig.HeartbeatData)
}

type NetReader interface {
	Read(after func(data []byte) bool)
}

func newNetReader(conn NetConn, serializer Serializer, dispatchers []*Dispatcher, hub NetHub) NetReader {
	reader := new(netReader)
	reader.conn = conn
	reader.serializer = serializer
	reader.dispatchers = dispatchers
	reader.hub = hub
	return reader
}

type netReader struct {
	conn        NetConn
	serializer  Serializer
	dispatchers []*Dispatcher
	hub         NetHub
}

func (r *netReader) Read(after func(data []byte) bool) {
	for {
		data, err := r.conn.Read()
		if err != nil {
			r.conn.Close()
			break
		}
		r.conn.Session().Set(SESSION_CONNECT_LAST_READ_TIME, time.Now())
		r.process(data, after)
	}
}

func (r *netReader) process(raw []byte, after func(data []byte) bool) {

	// safe the user handler to avoid the whole server down
	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Reader Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			log4g.Error("********************* Reader Panic *********************")
		}
	}()

	if after != nil {
		if !after(raw) {
			return
		}
	}

	v, h, rp, err := r.serializer.Deserialize(raw)
	if err != nil {
		return
	}

	Dispatch(r.dispatchers, newNetAgent(r.hub, r.conn, rp, v, h, r.serializer))
}
