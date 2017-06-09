package net4g

import "time"

var NetConfig struct {
	MessageLengthSize  int
	LittleEndian       bool
	HeartbeatFrequency time.Duration
	HeartbeatData      []byte
	NetTolerableTime   time.Duration
	MessageIdSize      int
	MessagePrefixSize  int
	KeepWriteData      bool
}

func init() {
	NetConfig.MessageLengthSize = 2
	NetConfig.LittleEndian = false
	NetConfig.HeartbeatFrequency = 10 * time.Second
	NetConfig.HeartbeatData = []byte{}
	NetConfig.NetTolerableTime = 3 * time.Second
	NetConfig.MessageIdSize = 2
}
