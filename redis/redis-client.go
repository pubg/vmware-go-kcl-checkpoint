package redis

import (
	"context"

	goredis "github.com/go-redis/redis/v8"
	"github.com/golang/glog"
)

var Nil = goredis.Nil

// NewRedisClient creates new redis client
func NewRedisClient(ctx context.Context, redisServer string) (*goredis.Client, error) {
	r := goredis.NewClient(&goredis.Options{
		Addr:     redisServer,
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	v, err := r.Info(ctx, "server").Result()
	if err != nil {
		return nil, err
	}

	glog.Info(v)
	return r, nil
}
