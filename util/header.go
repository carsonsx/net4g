package util

import (
	"encoding/binary"
)

func AddIntHeader(data []byte, headerSize int, header uint64, littleEndian bool) []byte {
	pack := make([]byte, headerSize+len(data))
	switch headerSize {
	case 1:
		pack[0] = byte(header)
	case 2:
		if littleEndian {
			binary.LittleEndian.PutUint16(pack, uint16(header))
		} else {
			binary.BigEndian.PutUint16(pack, uint16(header))
		}
	case 4:
		if littleEndian {
			binary.LittleEndian.PutUint32(pack, uint32(header))
		} else {
			binary.BigEndian.PutUint32(pack, uint32(header))
		}
	case 8:
		if littleEndian {
			binary.LittleEndian.PutUint64(pack, header)
		} else {
			binary.BigEndian.PutUint64(pack, header)
		}
	default:
		panic("wrong header size")
	}
	copy(pack[headerSize:], data)
	return pack
}

func GetIntHeader(data []byte, headerSize int, littleEndian bool) int64 {
	var header int64
	switch headerSize {
	case 1:
		header = int64(data[0])
	case 2:
		if littleEndian {
			header = int64(binary.LittleEndian.Uint16(data))
		} else {
			header = int64(binary.BigEndian.Uint16(data))
		}
	case 4:
		if littleEndian {
			header = int64(binary.LittleEndian.Uint32(data))
		} else {
			header = int64(binary.BigEndian.Uint32(data))
		}
	case 8:
		if littleEndian {
			header = int64(binary.LittleEndian.Uint64(data))
		} else {
			header = int64(binary.BigEndian.Uint64(data))
		}
	default:
		panic("wrong header length")
	}
	return header
}
