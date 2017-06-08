package net4g

import (
	"os"
	"syscall"
)

var Terminal os.Signal = syscall.SIGTERM

type Closer interface {
	Close()
}

type NetAddr struct {
	Name string
	Addr string
}

func NewAddr(addr string) *NetAddr {
	return NewNamedAddr(addr, addr)
}

func NewNamedAddr(name, addr string) *NetAddr {
	return &NetAddr{Name: name, Addr: addr}
}

func NewNetAddrFn(addr ...string) (func () (addrs []*NetAddr, err error)) {
	return func() (addrs []*NetAddr, err error) {
		for _, _addr := range addr {
			addrs = append(addrs, NewNamedAddr(_addr, _addr))
		}
		return
	}
}

func NewNetNamedAddrFn(name_or_addr ...string) (func () (addrs []*NetAddr, err error)) {
	return func() (addrs []*NetAddr, err error) {
		var _name string
		for i, _name_or_addr := range name_or_addr {
			if i % 2 == 0 {
				_name = _name_or_addr
				continue
			}
			addrs = append(addrs, NewNamedAddr(_name, _name_or_addr))
		}
		return
	}
}