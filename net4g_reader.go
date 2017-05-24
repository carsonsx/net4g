package net4g

import (
	"bytes"
)

var heartBeatDataLen = len(NetConfig.HeartbeatData)

func IsHeartbeatData(data []byte) bool {
	return len(data) == heartBeatDataLen && bytes.Equal(data, NetConfig.HeartbeatData)
}

type NetReader interface {
	Read(before func (), after func (data []byte))
}

func newNetReader(conn NetConn, serializer Serializer, dispatchers []*dispatcher, connMgr *NetManager) NetReader  {
	reader := new (netReader)
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

func (r *netReader) Read(before func (), after func (data []byte))  {

	req := newNetReq(nil, nil, r.conn.RemoteAddr(), r.conn.Session())
	res := newNetRes(r.conn, r.serializer)

	for {

		if before != nil {
			before()
		}

		data, err := r.conn.Read()
		if err != nil {
			r.conn.Close()
			break
		}

		if after != nil {
			after(data)
		}

		if IsHeartbeatData(data) {
			//log.Println("heartbeat")
			r.mgr.Heartbeat(r.conn)
			continue
		}

		v, err := r.serializer.Deserialize(data)
		if err != nil {
			continue
		}

		req.bytes = data
		req.msg = v

		Dispatch(r.dispatchers, req, res)
	}
}
