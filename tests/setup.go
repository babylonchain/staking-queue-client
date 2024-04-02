package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/rabbitmq/amqp091-go"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/babylonchain/staking-queue-client/client"
	"github.com/babylonchain/staking-queue-client/config"
	"github.com/babylonchain/staking-queue-client/queuemngr"
)

func setupTestQueueConsumer(t *testing.T, cfg *config.QueueConfig) *queuemngr.QueueManager {
	amqpURI := fmt.Sprintf("amqp://%s:%s@%s", cfg.QueueUser, cfg.QueuePassword, cfg.Url)
	conn, err := amqp091.Dial(amqpURI)
	require.NoError(t, err)
	defer conn.Close()
	err = purgeQueues(conn, []string{
		client.ActiveStakingQueueName,
		// TODO currently other queues not exist
	})
	require.NoError(t, err)

	// Start the actual queue processing in our codebase
	queues, err := queuemngr.NewQueueManager(cfg, zap.NewNop())
	require.NoError(t, err)

	return queues
}

// purgeQueues purges all messages from the given list of queues.
func purgeQueues(conn *amqp091.Connection, queues []string) error {
	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("failed to open a channel in test: %w", err)
	}
	defer ch.Close()

	for _, queue := range queues {
		_, err := ch.QueuePurge(queue, false)
		if err != nil {
			if strings.Contains(err.Error(), "no queue") {
				fmt.Printf("Queue '%s' not found, ignoring...\n", queue)
				continue // Ignore this error and proceed with the next queue
			}
			return fmt.Errorf("failed to purge queue in test %s: %w", queue, err)
		}
	}

	return nil
}

func sendTestMessage[T any](client client.QueueClient, data []T) error {
	for _, d := range data {
		jsonBytes, err := json.Marshal(d)
		if err != nil {
			return err
		}
		messageBody := string(jsonBytes)
		err = client.SendMessage(context.TODO(), messageBody)
		if err != nil {
			return fmt.Errorf("failed to publish a message to queue %s: %w", client.GetQueueName(), err)
		}
	}
	return nil
}

func buildActiveNStakingEvents(stakerHash string, numOfEvent int) []*client.ActiveStakingEvent {
	var activeStakingEvents []*client.ActiveStakingEvent
	for i := 0; i < numOfEvent; i++ {
		activeStakingEvent := &client.ActiveStakingEvent{
			EventType:             client.ActiveStakingEventType,
			StakingTxHashHex:      "0x1234567890abcdef" + fmt.Sprint(i),
			StakerPkHex:           stakerHash,
			FinalityProviderPkHex: "0xabcdef1234567890" + fmt.Sprint(i),
			StakingValue:          1 + uint64(i),
			StakingStartHeight:    100 + uint64(i),
			StakingStartTimestamp: time.Now().String(),
			StakingTimeLock:       200 + uint64(i),
			StakingOutputIndex:    1 + uint64(i),
			StakingTxHex:          "0xabcdef1234567890" + fmt.Sprint(i),
		}
		activeStakingEvents = append(activeStakingEvents, activeStakingEvent)
	}
	return activeStakingEvents
}

func buildNUnbondingEvents(stakerHash string, numOfEvent int) []*client.UnbondingStakingEvent {
	var unbondingEvents []*client.UnbondingStakingEvent
	for i := 0; i < numOfEvent; i++ {
		unbondingEv := &client.UnbondingStakingEvent{
			EventType:               client.UnbondingStakingEventType,
			StakingTxHashHex:        "0x1234567890abcdef" + fmt.Sprint(i),
			UnbondingStartHeight:    uint64(i),
			UnbondingStartTimestamp: time.Now().String(),
			UnbondingTimeLock:       200 + uint64(i),
			UnbondingOutputIndex:    uint64(0),
			UnbondingTxHex:          "0xabcdef1234567890" + fmt.Sprint(i),
			UnbondingTxHashHex:      "0x1234567890abcdef" + fmt.Sprint(i),
		}
		unbondingEvents = append(unbondingEvents, unbondingEv)
	}

	return unbondingEvents
}

func buildNWithdrawEvents(numOfEvent int) []*client.WithdrawStakingEvent {
	var withdrawEvents []*client.WithdrawStakingEvent
	for i := 0; i < numOfEvent; i++ {
		withdrawEv := &client.WithdrawStakingEvent{
			EventType:        client.WithdrawStakingEventType,
			StakingTxHashHex: "0x1234567890abcdef" + fmt.Sprint(i),
		}
		withdrawEvents = append(withdrawEvents, withdrawEv)
	}

	return withdrawEvents
}

func buildNExpiryEvents(numOfEvent int) []*client.ExpiredStakingEvent {
	var expiryEvents []*client.ExpiredStakingEvent
	for i := 0; i < numOfEvent; i++ {
		expiryEv := &client.ExpiredStakingEvent{
			EventType:        client.ExpiredStakingEventType,
			StakingTxHashHex: "0x1234567890abcdef" + fmt.Sprint(i),
		}
		expiryEvents = append(expiryEvents, expiryEv)
	}

	return expiryEvents
}
