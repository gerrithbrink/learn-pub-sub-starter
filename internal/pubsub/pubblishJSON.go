package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {

	jsonBytes, err := json.Marshal(val)
	if err != nil {
		return err
	}

	ctx := context.Background()

	ch.PublishWithContext(ctx, exchange, key, false, false, amqp.Publishing{ContentType: "application/json", Body: jsonBytes})

	return fmt.Errorf("error")
}
