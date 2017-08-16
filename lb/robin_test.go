package lb_test

import (
	"fmt"
	"github.com/carsonsx/log4g"
	"github.com/carsonsx/net4g/lb"
	"github.com/hashicorp/consul/api"
	"testing"
	"time"
)

var balance = new(lb.RoundRobinLoadBalance)

func addrFn() ([]string, error) {

	cli, err := api.NewClient(&api.Config{
		//Address: "consul-dev:8500",
		Address: "192.168.56.201:8500",
	})
	if err != nil {
		log4g.Error(err)
		return nil, err
	}
	entries, _, err := cli.Health().Service("myservice", "", true, nil)
	if err != nil {
		log4g.Error(err)
		return nil, err
	}
	var services []string
	for _, entry := range entries {
		services = append(services, fmt.Sprintf("%s:%d", entry.Service.Address, entry.Service.Port))
	}
	return services, nil
}

func TestRoundRobinLoadBalance_Select(t *testing.T) {

	balance.Start(addrFn, time.Second)

	for {
		log4g.Info(balance.Select())
		time.Sleep(time.Second)
	}

}

func TestRoundRobinLoadBalance_Remove(t *testing.T) {
	balance.Start(addrFn, 0)
	balance.Remove("192.168.0.101:80")
	log4g.Info(balance.Select())
	log4g.Info(balance.Select())
	balance.Remove("192.168.0.103:80")
	log4g.Info(balance.Select())
	log4g.Info(balance.Select())
	balance.Remove("192.168.0.102:80")
	log4g.Info(balance.Select())
	log4g.Info(balance.Select())
}
