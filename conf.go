package net4g

import (
	"fmt"
	"github.com/carsonsx/gutil"
)

type ReadMode string

const (
	READ_MODE_BY_LENGTH ReadMode = "by_length"
	READ_MODE_BEGIN_END ReadMode = "begin_end"
)

var NetConfig struct {
	ServerName             string   `json:"server_name"`
	Address                string   `json:"-"`
	Host                   string   `json:"host"`
	Port                   int      `json:"port"`
	ReadMode               ReadMode `json:"read_mode"`
	MaxLength              uint64   `json:"max_length"`
	BeginBytes             []byte   `json:"-"`
	EndBytes               []byte   `json:"-"`
	HeaderSize           int    `json:"header_size"`
	LengthIndex          int    `json:"length_index"`
	LengthSize           int    `json:"length_size"`
	LittleEndian         bool   `json:"little_endian"`
	DispatchChanSize     int    `json:"dispatch_chan_size"`
	WriteChanSize        int    `json:"write_chan_size"`
	HeartbeatFrequency   int    `json:"heartbeat_frequency"`
	HeartbeatData        []byte `json:"heartbeat_data"`
	NetTolerableTime     int    `json:"net_tolerable_time"`
	IdSize               int    `json:"id_size"`
	CacheWriteFailedData bool `json:"cache_write_failed_data"`
	TimeTickerInterval   int    `json:"time_event_check_interval"`
	MonitorBeat          int    `json:"monitor_beat"`
}

func init() {
	NetConfig.ServerName = "GameServer"
	NetConfig.Port = 6666
	NetConfig.ReadMode = READ_MODE_BY_LENGTH
	NetConfig.MaxLength = 1 << 16
	NetConfig.HeaderSize = 2
	NetConfig.LengthIndex = 0
	NetConfig.LengthSize = 2
	NetConfig.LittleEndian = false
	NetConfig.DispatchChanSize = 100
	NetConfig.WriteChanSize = 100
	NetConfig.HeartbeatFrequency = 10 //second
	NetConfig.HeartbeatData = []byte{}
	NetConfig.NetTolerableTime = 3 // second
	NetConfig.IdSize = 2
	NetConfig.TimeTickerInterval = 1000 // Millisecond
	NetConfig.MonitorBeat = 10          // second

	gutil.ListenFirstValidJsonFile(&NetConfig, nil, "net4g.json", "conf/net4g.json", "config/net4g.json")
	NetConfig.Address = fmt.Sprintf("%s:%d", NetConfig.Host, NetConfig.Port)
}
