package net4g

import (
	"os"
	"syscall"
)

var Terminal os.Signal = syscall.SIGTERM

var GlobalSerializer Serializer = NewJsonSerializer()

var GlobalDispatcher *dispatcher = NewDispatcher("net4g")


type Closer interface {
	Close()
}