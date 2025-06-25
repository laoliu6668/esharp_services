package esharp_services

import (
	"fmt"

	"github.com/laoliu6668/esharp_services/util/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	EXCHANGE_HTX     = "htx"
	EXCHANGE_BINANCE = "binance"
	EXCHANGE_BITGET  = "bitget"
)

var schemaCh *amqp091.Channel

var redisDB *redis.Client
var redisDB_H *redis.Client

func InitDB(db *redis.Client) {
	redisDB = db
}

func InitDB_H(db *redis.Client) {
	redisDB_H = db
}

func InitRabbitMq(s string) {
	rabbitmq.InitConn(s)
	initSchemaCh()
}

func initSchemaCh() {
	ch, err := rabbitmq.NewChannel()
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	err = ch.ExchangeDeclare(
		"hedge_schema_config", // name
		"direct",              // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		fmt.Printf("err: %v\n", err)
		return
	}
	schemaCh = ch
}

func PublishToHedgeSchema(input []byte) error {
	return schemaCh.Publish(
		"hedge_schema_config", // exchange
		"",                    // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        input,
		})
}

func PublishToFundingHedgeSchema(input []byte) error {
	return schemaCh.Publish(
		"funding_hedge_schema_config", // exchange
		"",                            // routing key
		false,                         // mandatory
		false,                         // immediate
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        input,
		})
}
