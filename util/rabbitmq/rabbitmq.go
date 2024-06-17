package rabbitmq

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var conn *amqp.Connection

func InitConn(target string) error {
	c, err := amqp.Dial(target)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %v\n", err)
		return err
	}
	conn = c
	return nil
}
func CloseConn() {
	conn.Close()
}
func NewChannel() (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %v\n", err)
		return nil, err
	}
	return ch, nil
}

func PublishToExchange(ch *amqp.Channel, message []byte, exchangeName, routingKey string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := ch.PublishWithContext(ctx,
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        message,
		},
	)
	if err != nil {
		fmt.Printf("Failed to publish a message: %v\n", err)
		return err
	}
	// fmt.Printf(" [x] Sent %s\n", body)
	return nil
}
