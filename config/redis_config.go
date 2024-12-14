package config

import (
	"github.com/redis/go-redis/v9"
)

func InitCache() *redis.Client {
	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	return redisClient
}
