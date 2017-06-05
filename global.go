package net4g

import (
	"os"
	"syscall"
)

var Terminal os.Signal = syscall.SIGTERM

var GlobalSerializer Serializer = NewJsonSerializer()

var ServerDispatcher *dispatcher = NewDispatcher("net4g")
var ClientDispatcher *dispatcher = NewDispatcher("net4g")


type Closer interface {
	Close()
}