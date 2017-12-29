package net4g

import (
	"os"
	"syscall"
)

const (
	HEART_BEAT_TIME     = "__NET4G01"
	SESSION_GROUP_NAME              = "__NET4G02"
	SESSION_CONNECT_KEY             = "__NET4G03"
	SESSION_CONNECT_KEY_USER        = "__NET4G03_USER"
	SESSION_CONNECT_ESTABLISH_TIME  = "__NET4G04"
	SESSION_CONNECT_LAST_READ_TIME  = "__NET4G05"
	SESSION_CONNECT_LAST_WRITE_TIME = "__NET4G06"
)

var Terminal os.Signal = syscall.SIGTERM

type Closer interface {
	Close()
}

