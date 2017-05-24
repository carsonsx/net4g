package net4g

import (
	"net"
)

type NetSession interface {
	SetString(key string, value string)
	GetString(key string, defaultValue ...string) string
	SetInt(key string, value int)
	GetInt(key string, defaultValue ...int) int
	SetInt64(key string, value int64)
	GetInt64(key string, defaultValue ...int64) int64
	SetBool(key string, value bool)
	GetBool(key string, defaultValue ...bool) bool
	SetData(key string, value interface{})
	GetData(key string, defaultValue ...interface{}) interface{}
}

func NewNetSession() NetSession {
	s := new(netSession)
	s.map_string = make(map[string]string)
	s.map_int = make(map[string]int)
	s.map_int64 = make(map[string]int64)
	s.map_bool = make(map[string]bool)
	s.map_data = make(map[string]interface{})
	return s
}

type netSession struct {
	map_string map[string]string
	map_int map[string]int
	map_int64 map[string]int64
	map_bool map[string]bool
	map_data map[string]interface{}
}

func (s *netSession) SetString(key string, value string) ()  {
	s.map_string[key] = value
}

func (s *netSession) GetString(key string, defaultValue ...string) string {
	if value, ok := s.map_string[key]; ok {
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return ""
	}
}

func (s *netSession) SetInt(key string, value int) ()  {
	s.map_int[key] = value
}

func (s *netSession) GetInt(key string, defaultValue ...int) int {
	if value, ok := s.map_int[key]; ok {
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return -1
	}
}

func (s *netSession) SetInt64(key string, value int64) ()  {
	s.map_int64[key] = value
}

func (s *netSession) GetInt64(key string, defaultValue ...int64) int64 {
	if value, ok := s.map_int64[key]; ok {
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return -1
	}
}

func (s *netSession) SetBool(key string, value bool) ()  {
	s.map_bool[key] = value
}

func (s *netSession) GetBool(key string, defaultValue ...bool) bool {
	if value, ok := s.map_bool[key]; ok {
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return false
	}
}

func (s *netSession) SetData(key string, value interface{}) ()  {
	s.map_data[key] = value
}

func (s *netSession) GetData(key string, defaultValue ...interface{}) interface{} {
	if value, ok := s.map_data[key]; ok {
		return value
	} else if len(defaultValue) > 0 {
		return defaultValue[0]
	} else {
		return nil
	}
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