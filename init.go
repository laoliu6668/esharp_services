package esharp_services

import (
	"bytes"
	"compress/gzip"
	"fmt"

	"github.com/laoliu6668/esharp_services/util/rabbitmq"
	"github.com/rabbitmq/amqp091-go"
	"github.com/redis/go-redis/v9"
)

const (
	EXCHANGE_HTX = "htx"
)

var schemaCh *amqp091.Channel
var swapAccountCh *amqp091.Channel

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
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	defer gw.Close()
	_, err := gw.Write(input)
	if err != nil {
		return err
	}
	if err := gw.Close(); err != nil {
		return err
	}
	return schemaCh.Publish(
		"hedge_schema_config", // exchange
		"",                    // routing key
		false,                 // mandatory
		false,                 // immediate
		amqp091.Publishing{
			ContentType: "text/plain",
			Body:        buf.Bytes(),
		})
}
