package esharp_services

import (
	"github.com/redis/go-redis/v9"
)

const (
	EXCHANGE_HTX = "htx"
)

var redisDB *redis.Client
var redisDB_H *redis.Client

func InitDB(db *redis.Client) {
	redisDB = db
}

func InitDB_H(db *redis.Client) {
	redisDB_H = db
}
