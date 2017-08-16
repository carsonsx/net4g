package net4g

import (
	"encoding/json"
	"github.com/carsonsx/log4g"
	"io/ioutil"
	"time"
)

type ReadMode string

const (
	READ_MODE_BY_LENGTH ReadMode = "by_length"
	READ_MODE_BEGIN_END ReadMode = "begin_end"
)

var NetConfig netConfig

type netConfig struct {
	ReadMode           ReadMode      `json:"read_mode"`
	MaxLength          uint64        `json:"max_length"`
	BeginBytes         []byte        `json:"-"`
	EndBytes           []byte        `json:"-"`
	HeaderSize         int           `json:"header_size"`
	LengthIndex        int           `json:"length_index"`
	LengthSize         int           `json:"length_size"`
	LittleEndian       bool          `json:"little_endian"`
	HeartbeatFrequency time.Duration `json:"heartbeat_frequency"`
	HeartbeatData      []byte        `json:"heartbeat_data"`
	NetTolerableTime   time.Duration `json:"net_tolerable_time"`
	IdSize             int           `json:"id_size"`
	KeepWriteData      bool          `json:"keep_write_data"`
}

func (c *netConfig) Print() {
	log4g.Info("Net4g Config:")
	log4g.Info(log4g.JsonFunc(&NetConfig))
}

var searchPath = []string{"", "conf/", "config/"}

func init() {
	NetConfig.ReadMode = READ_MODE_BY_LENGTH
	NetConfig.MaxLength = 1 << 16
	NetConfig.HeaderSize = 2
	NetConfig.LengthIndex = 0
	NetConfig.LengthSize = 2
	NetConfig.LittleEndian = false
	NetConfig.HeartbeatFrequency = 10 //second
	NetConfig.HeartbeatData = []byte{}
	NetConfig.NetTolerableTime = 3 // second
	NetConfig.IdSize = 2

	searchJsonConfig("net4g.json", &NetConfig)
}

func searchJsonConfig(filename string, v interface{}) {
	found := false
	for _, prefix := range searchPath {
		filepath := prefix + filename
		if loadJsonConfig(filepath, v) {
			found = true
			break
		}
	}
	if !found {
		log4g.Info("not found any net4g config")
	} else {
		NetConfig.Print()
	}
}

func loadJsonConfig(filename string, v interface{}) bool {
	log4g.Debug("try to load net4g config file: %s", filename)
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		log4g.Debug(err)
		return false
	}
	err = json.Unmarshal(data, v)
	if err != nil {
		log4g.Fatal(err)
		return false
	}
	log4g.Info("loaded net4g config file: %s", filename)
	return true
}
