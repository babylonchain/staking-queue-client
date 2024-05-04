package tests

import (
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

type TestServer struct {
	QueueManager *queuemngr.QueueManager
	Conn         *amqp091.Connection
}

func (ts *TestServer) Stop(t *testing.T) {
	err := ts.QueueManager.Stop()
	require.NoError(t, err)
	err = ts.Conn.Close()
	require.NoError(t, err)

}

func setupTestQueueConsumer(t *testing.T, cfg *config.QueueConfig) *TestServer {
	amqpURI := fmt.Sprintf("amqp://%s:%s@%s", cfg.QueueUser, cfg.QueuePassword, cfg.Url)
	conn, err := amqp091.Dial(amqpURI)
	require.NoError(t, err)
	err = purgeQueues(conn, []string{
		client.ActiveStakingQueueName,
		client.UnbondingStakingQueueName,
		client.WithdrawStakingQueueName,
		client.ExpiredStakingQueueName,
		client.StakingStatsQueueName,
		// purge delay queues too
		client.ActiveStakingQueueName + "_delay",
		client.UnbondingStakingQueueName + "_delay",
		client.WithdrawStakingQueueName + "_delay",
		client.ExpiredStakingQueueName + "_delay",
		client.StakingStatsQueueName + "_delay",
	})
	require.NoError(t, err)

	// Start the actual queue processing in our codebase
	queues, err := queuemngr.NewQueueManager(cfg, zap.NewNop())
	require.NoError(t, err)

	return &TestServer{
		QueueManager: queues,
		Conn:         conn,
	}
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
			if strings.Contains(err.Error(), "NOT_FOUND") || strings.Contains(err.Error(), "channel/connection is not open") {
				continue
			}
			return fmt.Errorf("failed to purge queue in test %s: %w", queue, err)
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
			StakingValue:          1 + int64(i),
			StakingStartHeight:    100 + uint64(i),
			StakingStartTimestamp: time.Now().Unix(),
			StakingTimeLock:       200 + uint64(i),
			StakingOutputIndex:    1 + uint64(i),
			StakingTxHex:          "0xabcdef1234567890" + fmt.Sprint(i),
		}
		activeStakingEvents = append(activeStakingEvents, activeStakingEvent)
	}
	return activeStakingEvents
}

func buildNUnbondingEvents(numOfEvent int) []*client.UnbondingStakingEvent {
	var unbondingEvents []*client.UnbondingStakingEvent
	for i := 0; i < numOfEvent; i++ {
		unbondingEv := &client.UnbondingStakingEvent{
			EventType:               client.UnbondingStakingEventType,
			StakingTxHashHex:        "0x1234567890abcdef" + fmt.Sprint(i),
			UnbondingStartHeight:    uint64(i),
			UnbondingStartTimestamp: time.Now().Unix(),
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
			TxType:           "active",
		}
		expiryEvents = append(expiryEvents, expiryEv)
	}

	return expiryEvents
}

// inspectQueueMessageCount inspects the number of messages in the given queue.
func inspectQueueMessageCount(t *testing.T, conn *amqp091.Connection, queueName string) (int, error) {
	ch, err := conn.Channel()
	if err != nil {
		t.Fatalf("failed to open a channel in test: %v", err)
	}

	q, err := ch.QueueDeclarePassive(queueName, false, false, false, false, nil)
	if err != nil {
		if strings.Contains(err.Error(), "NOT_FOUND") || strings.Contains(err.Error(), "channel/connection is not open") {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to inspect queue in test %s: %w", queueName, err)
	}
	return q.Messages, nil
}
