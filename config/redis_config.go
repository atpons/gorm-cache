package config

import (
	"sync"

	"github.com/redis/rueidis"
)

type RedisConfigMode int

const (
	RedisConfigModeOptions RedisConfigMode = 0
	RedisConfigModeRaw     RedisConfigMode = 1
)

type RedisConfig struct {
	Mode RedisConfigMode

	Options rueidis.ClientOption
	Client  rueidis.Client

	once sync.Once
}

func (c *RedisConfig) InitClient() rueidis.Client {
	c.once.Do(func() {
		if c.Mode == RedisConfigModeOptions {
			client, err := rueidis.NewClient(c.Options)
			if err != nil {
				panic(err)
			}
			c.Client = client
		}
	})
	return c.Client
}
