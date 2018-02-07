package net4g

import "sync"

type byteSlice struct {
	data [][]byte
}

var NetCache netCache

type netCache struct{
	cacheData sync.Map
}

func (c *netCache) Add(key string, data []byte) {
	if NetConfig.CacheWriteFailedData && !IsHeartbeatData(data) {
		var cache *byteSlice
		if value, ok := c.cacheData.Load(key); !ok {
			cache = new (byteSlice)
			c.cacheData.Store(key, cache)
		} else {
			cache = value.(*byteSlice)
		}
		cache.data = append(cache.data, data)
	}
}

func (c *netCache) Clear(key string) {
	if value, ok := c.cacheData.Load(key); ok {
		c.cacheData.Delete(key)
		cache := value.(*byteSlice)
		cache.data = [][]byte{}
	}
}

func (c *netCache) Write(key string, conn NetConn) {
	if value, ok := c.cacheData.Load(key); ok {
		c.cacheData.Delete(key)
		cache := value.(*byteSlice)
		for _, data := range cache.data {
			conn.Write(data)
		}
	}
}