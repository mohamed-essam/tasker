package tasker

import (
	"fmt"
	"time"

	redis "github.com/go-redis/redis"
)

var _redis iRedis

type iRedis interface {
	LPush(key string, values ...interface{}) *redis.IntCmd
	BRPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd
	HSet(key, field string, value interface{}) *redis.BoolCmd
	SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd
	Del(keys ...string) *redis.IntCmd
	HGet(key, field string) *redis.StringCmd
	HDel(key string, fields ...string) *redis.IntCmd
}

func redisClient() iRedis {
	if _redis == nil {
		_redis = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%s", config().RedisHost, config().RedisPort),
			Password: "",
			DB:       config().RedisDB,
		})
	}

	return _redis
}
