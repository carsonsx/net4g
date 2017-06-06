package net4g

import (
	"bytes"
	"github.com/carsonsx/log4g"
	"runtime/debug"
)

func IsHeartbeatData(data []byte) bool {
	return len(data) == len(NetConfig.HeartbeatData) && bytes.Equal(data, NetConfig.HeartbeatData)
}

type NetReader interface {
	Read(after func(data []byte) bool)
}

func newNetReader(conn NetConn, serializer Serializer, dispatchers []*dispatcher, connMgr *NetManager) NetReader {
	reader := new(netReader)
	reader.conn = conn
	reader.serializer = serializer
	reader.dispatchers = dispatchers
	reader.mgr = connMgr
	return reader
}

type netReader struct {
	conn        NetConn
	serializer  Serializer
	dispatchers []*dispatcher
	mgr         *NetManager
}

func (r *netReader) Read(after func(data []byte) bool) {
	for {
		data, err := r.conn.Read()
		if err != nil {
			r.conn.Close()
			break
		}
		r.read(data, after)
	}
}

func (r *netReader) read(data []byte, after func(data []byte) bool) {

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
		if !after(data) {
			return
		}
	}

	v, err := r.serializer.Deserialize(data)
	if err != nil {
		return
	}

	req := newNetReq(data, v, r.conn.RemoteAddr(), r.conn.Session())
	res := newNetRes(r.conn, r.serializer)

	Dispatch(r.dispatchers, req, res)
}
