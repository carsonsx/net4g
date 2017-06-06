package net4g

import (
	"os"
	"syscall"
)

var Terminal os.Signal = syscall.SIGTERM

type Closer interface {
	Close()
}