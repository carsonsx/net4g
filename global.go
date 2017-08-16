package net4g

import (
	"os"
	"syscall"
	"time"
)

const (
	HEART_BEAT_INTERVAL  = 1 * time.Second
	HEART_BEAT_LAST_TIME = "__NET4G01"
	//SESSION_ID = "ID"
	SESSION_GROUP_NAME              = "__NET4G02"
	SESSION_CONNECT_KEY             = "__NET4G03"
	SESSION_CONNECT_ESTABLISH_TIME  = "__NET4G04"
	SESSION_CONNECT_LAST_READ_TIME  = "__NET4G05"
	SESSION_CONNECT_LAST_WRITE_TIME = "__NET4G06"
)

var Terminal os.Signal = syscall.SIGTERM

type Closer interface {
	Close()
}

type NetAddr struct {
	Key  string
	Addr string
}

func NewAddr(addr string) *NetAddr {
	return NewKeyAddr(addr, addr)
}

func NewKeyAddr(key, addr string) *NetAddr {
	return &NetAddr{Key: key, Addr: addr}
}

func NewNetAddrFn(addr ...string) func() (addrs []*NetAddr, err error) {
	return func() (addrs []*NetAddr, err error) {
		for _, _addr := range addr {
			addrs = append(addrs, NewKeyAddr(_addr, _addr))
		}
		return
	}
}

func NewNetKeyAddrFn(key_or_addr ...string) func() (addrs []*NetAddr, err error) {
	return func() (addrs []*NetAddr, err error) {
		var _key string
		for i, _key_or_addr := range key_or_addr {
			if i%2 == 0 {
				_key = _key_or_addr
				continue
			}
			addrs = append(addrs, NewKeyAddr(_key, _key_or_addr))
		}
		return
	}
}
