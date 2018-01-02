package net4g

import (
	"sync"
	"time"
)

type LoadBalance interface {
	Select() string
	Remove(seed string)
}

type RoundRobinLoadBalance struct {
	seeds    []string
	size     int
	round    int
	seedFunc func() ([]string, error)
	sync.Mutex
}

func (lb *RoundRobinLoadBalance) Select() string {
	lb.Lock()
	defer lb.Unlock()
	if lb.size == 0 {
		return ""
	}
	if lb.round >= lb.size {
		lb.round = 0
	}
	client := lb.seeds[lb.round]
	lb.round++
	return client
}

func (lb *RoundRobinLoadBalance) Remove(seed string) {
	lb.Lock()
	defer lb.Unlock()
	for i, v := range lb.seeds {
		if v == seed {
			lb.seeds = append(lb.seeds[:i], lb.seeds[i+1:]...)
			break
		}
	}
	lb.size = len(lb.seeds)
}

func (lb *RoundRobinLoadBalance) Start(seedFunc func() ([]string, error), duration time.Duration) {
	lb.seedFunc = seedFunc
	lb.refresh()
	if duration > 0 {
		go func() {
			ticker := time.NewTicker(duration)
			for {
				<-ticker.C
				lb.refresh()
			}
		}()
	}
}

func (lb *RoundRobinLoadBalance) refresh() {
	lb.Lock()
	defer lb.Unlock()
	selects, err := lb.seedFunc()
	if err == nil {
		lb.seeds = selects
		lb.size = len(lb.seeds)
	}
}
