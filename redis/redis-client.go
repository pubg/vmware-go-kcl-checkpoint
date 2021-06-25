package redis

import (
	goredis "github.com/go-redis/redis"
	"github.com/golang/glog"
)

var Nil = goredis.Nil

// New creates new redis client
func NewRedisClient(redisServer string) (*goredis.Client, error) {
	r := goredis.NewClient(&goredis.Options{
		Addr:     redisServer,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	v, err := r.Info("server").Result()
	if err != nil {
		return nil, err
	}

	glog.Info(v)
	return r, nil
}
