package util

import "time"

func SmartSleep(d time.Duration, max time.Duration) time.Duration {
	if d == 0 {
		panic("time must be more than 0")
	}
	time.Sleep(d)
	d = d * 2
	if d > max {
		return max
	}
	return d
}
