package client

import (
	"context"
	"fmt"
	"strconv"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMqClient struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	queueName  string
	stopCh     chan struct{} // This is used to gracefully stop the message receiving loop
}

func NewRabbitMqClient(queueURL, user, pass, queueName string) (*RabbitMqClient, error) {
	amqpURI := fmt.Sprintf("amqp://%s:%s@%s", user, pass, queueURL)

	conn, err := amqp.Dial(amqpURI)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// Declare a queue that will be created if not exists
	_, err = ch.QueueDeclare(
		queueName, // name
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		return nil, err
	}

	err = ch.Confirm(false)
	if err != nil {
		return nil, err
	}

	return &RabbitMqClient{
		connection: conn,
		channel:    ch,
		queueName:  queueName,
		stopCh:     make(chan struct{}),
	}, nil
}

func (c *RabbitMqClient) ReceiveMessages() (<-chan QueueMessage, error) {
	msgs, err := c.channel.Consume(
		c.queueName, // queueName
		"",          // consumer
		false,       // auto-ack. We want to manually acknowledge the message after processing it.
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)
	if err != nil {
		return nil, err
	}
	output := make(chan QueueMessage)
	go func() {
		defer close(output)
		for {
			select {
			case d, ok := <-msgs:
				if !ok {
					return // Channel closed, exit goroutine
				}
				currentAttempt := d.Headers["x-processing-attempts"].(int)

				output <- QueueMessage{
					Body:          string(d.Body),
					Receipt:       strconv.FormatUint(d.DeliveryTag, 10),
					RetryAttempts: currentAttempt,
				}
			case <-c.stopCh:
				return // Stop signal received, exit goroutine
			}
		}
	}()

	return output, nil
}

// DeleteMessage deletes a message from the queue. In RabbitMQ, this is equivalent to acknowledging the message.
// The deliveryTag is the unique identifier for the message.
func (c *RabbitMqClient) DeleteMessage(deliveryTag string) error {
	deliveryTagInt, err := strconv.ParseUint(deliveryTag, 10, 64)
	if err != nil {
		return err
	}
	return c.channel.Ack(deliveryTagInt, false)
}

// ReQueueMessage requeues a message back to the queue. This is done by sending the message again with an incremented counter.
// The original message is then deleted from the queue.
func (c *RabbitMqClient) ReQueueMessage(ctx context.Context, message QueueMessage) error {
	err := c.sendMessageWithAttempts(ctx, message.Body, message.IncrementRetryAttempts())
	if err != nil {
		return fmt.Errorf("failed to requeue message: %w", err)
	}

	err = c.DeleteMessage(message.Receipt)
	if err != nil {
		return fmt.Errorf("failed to delete message while requeuing: %w", err)
	}

	return nil
}

// SendMessage sends a message to the queue. the ctx is used to control the timeout of the operation.
func (c *RabbitMqClient) sendMessageWithAttempts(ctx context.Context, messageBody string, attempts int) error {
	// Ensure the channel is open
	if c.channel == nil {
		return fmt.Errorf("RabbitMQ channel not initialized")
	}

	// Prepare new headers with the incremented counter
	newHeaders := amqp.Table{
		"x-processing-attempts": attempts,
	}

	// Publish a message to the queue
	confirmation, err := c.channel.PublishWithDeferredConfirmWithContext(
		ctx,
		"",          // exchange: Use the default exchange
		c.queueName, // routing key: The queue name
		true,        // mandatory: true indicates the server must route the message to a queue, otherwise error
		false,       // immediate: false indicates the server may wait to send the message until a consumer is available
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(messageBody),
			Headers:      newHeaders,
		},
	)

	if err != nil {
		return fmt.Errorf("failed to publish a message to queue %s: %w", c.queueName, err)
	}

	if confirmation == nil {
		return fmt.Errorf("message not confirmed when publishing into queue %s", c.queueName)
	}
	confirmed, err := confirmation.WaitContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to confirm message when publishing into queue %s: %w", c.queueName, err)
	}
	if !confirmed {
		return fmt.Errorf("message not confirmed when publishing into queue %s", c.queueName)
	}

	return nil
}

// SendMessage sends a message to the queue. the ctx is used to control the timeout of the operation.
func (c *RabbitMqClient) SendMessage(ctx context.Context, messageBody string) error {
	return c.sendMessageWithAttempts(ctx, messageBody, 0)
}

// Stop stops the message receiving process.
func (c *RabbitMqClient) Stop() error {
	close(c.stopCh) // Signal to stop receiving messages

	if err := c.channel.Close(); err != nil {
		return err
	}
	if err := c.connection.Close(); err != nil {
		return err
	}

	return nil
}

func (c *RabbitMqClient) GetQueueName() string {
	return c.queueName
}
