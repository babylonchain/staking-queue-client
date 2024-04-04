package client

import "context"

type QueueMessage struct {
	Body          string
	Receipt       string
	RetryAttempts int
}

func (m QueueMessage) IncrementRetryAttempts() int {
	m.RetryAttempts++
	return m.RetryAttempts
}

func (m QueueMessage) GetRetryAttempts() int {
	return m.RetryAttempts
}

// A common interface for queue clients regardless if it's a SQS, RabbitMQ, etc.
type QueueClient interface {
	SendMessage(ctx context.Context, messageBody string) error
	ReceiveMessages() (<-chan QueueMessage, error)
	DeleteMessage(receipt string) error
	Stop() error
	GetQueueName() string
	ReQueueMessage(receipt string) error
}

func NewQueueClient(queueURL, user, pass, queueName string) (QueueClient, error) {
	return NewRabbitMqClient(queueURL, user, pass, queueName)
}
