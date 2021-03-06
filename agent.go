package net4g

import (
	"github.com/carsonsx/gutil"
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
	SetLogin()
	IsLogin() bool
}

func NewNetSession() NetSession {
	s := new(netSession)
	s.map_data = gutil.NewSafeMap()
	return s
}

type netSession struct {
	map_data gutil.Map
	login    bool
}

func (s *netSession) Set(key string, value interface{}) {
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

func (s *netSession) Remove(key string) {
	s.map_data.Remove(key)
}

func (s *netSession) SetLogin() {
	s.login = true
}

func (s *netSession) IsLogin() bool {
	return s.login
}

type NetAgent interface {
	RawPack() *RawPack
	Header() interface{}
	Msg() interface{}
	RemoteAddr() net.Addr
	Session() NetSession
	Key(key ...string) string
	Write(v interface{}, h ...interface{}) error
	Close()
	IsClosed() bool
}

func newNetAgent(hub NetHub, conn NetConn, rp *RawPack, msg, header interface{}, serializer Serializer) *netAgent {
	agent := new(netAgent)
	agent.hub = hub
	agent.conn = conn
	agent.rp = rp
	agent.msg = msg
	agent.header = header
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
	header     interface{}
	remoteAddr net.Addr
	session    NetSession
	serializer Serializer
}

func (a *netAgent) RawPack() *RawPack {
	return a.rp
}

func (a *netAgent) Header() interface{} {
	return a.header
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

func (a *netAgent) Key(key ...string) string {
	if len(key) > 0 {
		a.hub.Key(a.conn, key[0])
		return key[0]
	} else {
		return a.session.GetString(SESSION_CONNECT_KEY_USER)
	}
}

func (a *netAgent) Write(v interface{}, h ...interface{}) error {
	data, err := Serialize(a.serializer, v, h...)
	if err != nil {
		return err
	}
	return a.conn.Write(data)
}

func (a *netAgent) Close() {
	a.conn.Close()
}

func (a *netAgent) IsClosed() bool {
	return a.conn.IsClosed()
}
