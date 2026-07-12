package pubsub

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T),
) error {

	chn, _, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	deliveryChan, err := chn.Consume(queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	for delivery := range deliveryChan {
		var data T
		err = json.Unmarshal(delivery.Body, data)
		if err != nil {
			return err
		}
		handler(data)
		delivery.Ack(false)

	}

	return nil
}
