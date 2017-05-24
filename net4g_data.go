package net4g

import (
	"net"
	"github.com/carsonsx/net4g/util"
)

type NetSession interface {
	SetValue(key string, value interface{})
	GetString(key string, defaultValue ...string) string
	GetInt(key string, defaultValue ...int) int
	GetInt64(key string, defaultValue ...int64) int64
	GetBool(key string, defaultValue ...bool) bool
	GetValue(key string, defaultValue ...interface{}) interface{}
	RemoveValue(key string)
}

func NewNetSession() NetSession {
	s := new(netSession)
	s.map_data = util.NewConcurrentMap()
	return s
}

type netSession struct {
	map_data util.Map
}

func (s *netSession) GetString(key string, defaultValue ...string) string {
	if value := s.map_data.Get(key); value != nil{
		return value.(string)
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return ""
	}
}

func (s *netSession) GetInt(key string, defaultValue ...int) int {
	if value := s.map_data.Get(key); value != nil{
		return value.(int)
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return -1
	}
}

func (s *netSession) GetInt64(key string, defaultValue ...int64) int64 {
	if value := s.map_data.Get(key); value != nil{
		return value.(int64)
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return -1
	}
}


func (s *netSession) GetBool(key string, defaultValue ...bool) bool {
	if value := s.map_data.Get(key); value != nil{
		return value.(bool)
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return false
	}
}

func (s *netSession) SetValue(key string, value interface{}) ()  {
	s.map_data.Put(key, value)
}

func (s *netSession) GetValue(key string, defaultValue ...interface{}) interface{} {
	if value := s.map_data.Get(key); value != nil{
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return nil
	}
}

func (s *netSession) RemoveValue(key string) ()  {
	s.map_data.Remove(key)
}

type NetReq interface {
	Bytes()      []byte
	Msg()        interface{}
	RemoteAddr() net.Addr
	Session()    NetSession
}

func newNetReq(bytes []byte, msg interface{}, remoteAddr net.Addr, session NetSession) *netReq {
	req := new(netReq)
	req.bytes = bytes
	req.msg = msg
	req.remoteAddr = remoteAddr
	req.session = session
	return req
}

type netReq struct {
	bytes     []byte
	msg        interface{}
	remoteAddr net.Addr
	session   NetSession
}

func (req *netReq) Bytes() []byte {
	return req.bytes
}

func (req *netReq) Msg() interface{} {
	return req.msg
}

func (req *netReq) RemoteAddr() net.Addr {
	return req.remoteAddr
}

func (req *netReq) Session() NetSession {
	return req.session
}


type NetRes interface {
	Write(v interface{}) error
	Close()
}

func newNetRes(conn NetConn, serializer Serializer) *netRes {
	res := new(netRes)
	res.conn = conn
	res.serializer = serializer
	return res
}

type netRes struct {
	conn       NetConn
	serializer Serializer
}

func (res *netRes) Write(v interface{}) error {
	data, err := res.serializer.Serialize(v)
	if err != nil {
		return err
	}
	res.conn.Write(data)
	return nil
}

func (res *netRes) Close() {
	res.conn.Close()
}