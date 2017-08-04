package net4g

import (
	"github.com/carsonsx/net4g/util"
	"net"
)

type NetSession interface {
	Set(key string, value interface{})
	Get(key string, defaultValue ...interface{}) interface{}
	GetString(key string, defaultValue ...string) string
	GetInt(key string, defaultValue ...int) int
	GetInt64(key string, defaultValue ...int64) int64
	GetBool(key string, defaultValue ...bool) bool
	Has(key string) bool
	Remove(key string)
}

func NewNetSession() NetSession {
	s := new(netSession)
	s.map_data = util.NewSafeMap()
	return s
}

type netSession struct {
	map_data util.Map
}

func (s *netSession) Set(key string, value interface{}) ()  {
	s.map_data.Put(key, value)
}

func (s *netSession) Get(key string, defaultValue ...interface{}) interface{} {
	if value := s.map_data.Get(key); value != nil {
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return nil
	}
}

func (s *netSession) GetString(key string, defaultValue ...string) string {
	if value := s.map_data.Get(key); value != nil {
		return value.(string)
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return ""
	}
}

func (s *netSession) GetInt(key string, defaultValue ...int) int {
	if value := s.map_data.Get(key); value != nil {
		return value.(int)
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return -1
	}
}

func (s *netSession) GetInt64(key string, defaultValue ...int64) int64 {
	if value := s.map_data.Get(key); value != nil {
		return value.(int64)
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return -1
	}
}

func (s *netSession) GetBool(key string, defaultValue ...bool) bool {
	if value := s.map_data.Get(key); value != nil {
		return value.(bool)
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return false
	}
}

func (s *netSession) Has(key string) bool {
	return s.map_data.Has(key)
}

func (s *netSession) Remove(key string) ()  {
	s.map_data.Remove(key)
}

type NetAgent interface {
	RawPack() *RawPack
	Msg() interface{}
	RemoteAddr() net.Addr
	Session() NetSession
	Key(key string)
	Write(v interface{}, prefix ...byte) error
	Close()
}

func newNetAgent(hub NetHub, conn NetConn, rp *RawPack, msg interface{}, serializer Serializer) *netAgent {
	agent := new(netAgent)
	agent.hub = hub
	agent.conn = conn
	agent.rp = rp
	agent.msg = msg
	agent.remoteAddr = conn.RemoteAddr()
	agent.session = conn.Session()
	agent.serializer = serializer
	return agent
}

type netAgent struct {
	hub        NetHub
	conn       NetConn
	prefix     []byte
	rp         *RawPack
	msg        interface{}
	remoteAddr net.Addr
	session    NetSession
	serializer Serializer
}

func (a *netAgent) RawPack() *RawPack {
	return a.rp
}

func (a *netAgent) Msg() interface{} {
	return a.msg
}

func (a *netAgent) RemoteAddr() net.Addr {
	return a.remoteAddr
}

func (a *netAgent) Session() NetSession {
	return a.session
}

func (a *netAgent) Key(key string) {
	a.hub.Key(a.conn, key)
}

func (a *netAgent) Write(v interface{}, prefix ...byte) error {
	data, err := Serialize(a.serializer, v, prefix...)
	if err != nil {
		return err
	}
	return a.conn.Write(data)
}

func (a *netAgent) Close() {
	a.conn.Close()
}
