package lb

import (
	"sync"
	"time"
)

type RoundRobinLoadBalance struct {
	selects []string
	size    int
	round   int
	seedFunc    func() ([]string, error)
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
	client := lb.selects[lb.round]
	lb.round++
	return client
}

func (lb *RoundRobinLoadBalance) Remove(sel string) {
	lb.Lock()
	defer lb.Unlock()
	for i, v := range lb.selects {
		if v == sel {
			lb.selects = append(lb.selects[:i], lb.selects[i+1:]...)
			break
		}
	}
	lb.size = len(lb.selects)
}

func (lb *RoundRobinLoadBalance) Start(seedFunc func() ([]string, error),  duration time.Duration) {
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
		lb.selects = selects
		lb.size = len(lb.selects)
	}
}



