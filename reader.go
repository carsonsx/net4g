package net4g

import (
	"github.com/carsonsx/log4g"
	"runtime/debug"
	"time"
)

type NetReader interface {
	Read(after func(data []byte) bool)
}

func newNetReader(conn NetConn, serializer Serializer, forwarder Forwarder, dispatchers []*Dispatcher, hub NetHub) NetReader {
	reader := new(netReader)
	reader.conn = conn
	reader.serializer = serializer
	reader.forwarder = forwarder
	reader.dispatchers = dispatchers
	reader.hub = hub
	return reader
}

type netReader struct {
	conn        NetConn
	serializer  Serializer
	forwarder  Forwarder
	dispatchers []*Dispatcher
	hub         NetHub
}

func (r *netReader) Read(prehandler func(data []byte) bool) {
	for {
		data, err := r.conn.Read()
		if err != nil {
			r.conn.Close()
			break
		}
		r.conn.Session().Set(SESSION_CONNECT_LAST_READ_TIME, time.Now())
		r.process(data, prehandler)
	}
}

func (r *netReader) process(raw []byte, prehandler func(data []byte) bool) {

	// safe the user handler to avoid the whole server down
	defer func() {
		if r := recover(); r != nil {
			log4g.Error("********************* Reader Panic *********************")
			log4g.Error(r)
			log4g.Error(string(debug.Stack()))
			log4g.Error("********************* Reader Panic *********************")
		}
	}()

	if prehandler != nil {
		if !prehandler(raw) {
			return
		}
	}

	rp := new(RawPack)
	var err error
	rp.Id, rp.Type, rp.Data, err = r.serializer.DeserializeId(raw)
	if err != nil {
		return
	}

	if r.forwarder != nil {
		if !r.forwarder.Handle(rp.Type, newNetAgent(r.hub, r.conn, rp, nil, nil, r.serializer)) {
			return
		}
	}

	var h interface{}
	var v interface{}
	if NeedDeserialize(r.dispatchers, rp.Type) {
		h, v, err = r.serializer.DeserializeData(rp.Data, rp.Type)
		if err != nil {
			return
		}
	} else {
		log4g.Debug("no need deserialize for %v", rp.Type)
	}

	if rp.Type != nil {
		log4g.Debug("dispatching %v", rp.Type)
	} else if rp.Id != nil {
		log4g.Debug("dispatching %v", rp.Id)
	} else {
		log4g.Debug("dispatching...")
	}
	Dispatch(r.dispatchers, newNetAgent(r.hub, r.conn, rp, v, h, r.serializer))
}
