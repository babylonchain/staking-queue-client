package tests

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/babylonchain/staking-queue-client/client"
	"github.com/babylonchain/staking-queue-client/config"
)

const (
	mockStakerHash = "0x1234567890abcdef"
)

func TestPing(t *testing.T) {
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	err := queueManager.Ping();
	require.NoError(t, err, "Ping should not return an error")
}

func TestStakingEvent(t *testing.T) {
	numStakingEvents := 3
	activeStakingEvents := buildActiveNStakingEvents(mockStakerHash, numStakingEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	stakingEventReceivedChan, err := queueManager.StakingQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range activeStakingEvents {
		err = queueManager.PushStakingEvent(ev)
		require.NoError(t, err)

		receivedEv := <-stakingEventReceivedChan
		var stakingEv client.ActiveStakingEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &stakingEv)
		require.NoError(t, err)
		require.Equal(t, ev, &stakingEv)
	}
}

func TestUnbondingEvent(t *testing.T) {
	numUnbondingEvents := 3
	unbondingEvents := buildNUnbondingEvents(numUnbondingEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	unbondingEvReceivedChan, err := queueManager.UnbondingQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range unbondingEvents {
		err = queueManager.PushUnbondingEvent(ev)
		require.NoError(t, err)

		receivedEv := <-unbondingEvReceivedChan
		var unbondingEv client.UnbondingStakingEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &unbondingEv)
		require.NoError(t, err)
		require.Equal(t, ev, &unbondingEv)
	}
}

func TestWithdrawEvent(t *testing.T) {
	numWithdrawEvents := 3
	withdrawEvents := buildNWithdrawEvents(numWithdrawEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	withdrawEventsReceivedChan, err := queueManager.WithdrawQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range withdrawEvents {
		err = queueManager.PushWithdrawEvent(ev)
		require.NoError(t, err)

		receivedEv := <-withdrawEventsReceivedChan
		var withdrawEv client.WithdrawStakingEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &withdrawEv)
		require.NoError(t, err)
		require.Equal(t, ev, &withdrawEv)
	}
}

func TestExpiryEvent(t *testing.T) {
	numExpiryEvents := 3
	expiryEvents := buildNExpiryEvents(numExpiryEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	expiryEventsReceivedChan, err := queueManager.ExpiryQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range expiryEvents {
		err = queueManager.PushExpiryEvent(ev)
		require.NoError(t, err)

		receivedEv := <-expiryEventsReceivedChan
		var expiryEvent client.ExpiredStakingEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &expiryEvent)
		require.NoError(t, err)
		require.Equal(t, ev, &expiryEvent)
	}
}

func TestBtcInfoEvent(t *testing.T) {
	numBtcInfoEvents := 3
	BtcInfoEvents := buildNBtcInfoEvents(numBtcInfoEvents)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	BtcInfoEventsReceivedChan, err := queueManager.BtcInfoQueue.ReceiveMessages()
	require.NoError(t, err)

	for _, ev := range BtcInfoEvents {
		err = queueManager.PushBtcInfoEvent(ev)
		require.NoError(t, err)

		receivedEv := <-BtcInfoEventsReceivedChan
		var BtcInfoEvent client.BtcInfoEvent
		err := json.Unmarshal([]byte(receivedEv.Body), &BtcInfoEvent)
		require.NoError(t, err)
		require.Equal(t, ev, &BtcInfoEvent)
	}
}

func TestReQueueEvent(t *testing.T) {
	activeStakingEvents := buildActiveNStakingEvents(mockStakerHash, 1)
	queueCfg := config.DefaultQueueConfig()

	testServer := setupTestQueueConsumer(t, queueCfg)
	defer testServer.Stop(t)

	queueManager := testServer.QueueManager

	stakingEventReceivedChan, err := queueManager.StakingQueue.ReceiveMessages()
	require.NoError(t, err)

	ev := activeStakingEvents[0]
	err = queueManager.PushStakingEvent(ev)
	require.NoError(t, err)

	var receivedEv client.QueueMessage

	select {
	case receivedEv = <-stakingEventReceivedChan:
	case <-time.After(10 * time.Second): // Wait up to 10 seconds for a message
		t.Fatal("timeout waiting for staking event")
	}

	var stakingEv client.ActiveStakingEvent
	err = json.Unmarshal([]byte(receivedEv.Body), &stakingEv)
	require.NoError(t, err)
	require.Equal(t, ev, &stakingEv)
	require.Equal(t, int32(0), receivedEv.RetryAttempts)

	// Now let's requeue the event
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	err = queueManager.StakingQueue.ReQueueMessage(ctx, receivedEv)
	require.NoError(t, err)
	time.Sleep(1 * time.Second) // Wait to ensure message has time to move to delayed queue

	// Check that the main queue is empty
	count, err := inspectQueueMessageCount(t, testServer.Conn, client.ActiveStakingQueueName)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	// Make sure it appears in the delayed queue
	delayedQueueCount, err := inspectQueueMessageCount(t, testServer.Conn, client.ActiveStakingQueueName+"_delay")
	require.NoError(t, err)
	require.Equal(t, 1, delayedQueueCount)

	// Checking delayed queue message appearance
	select {
	case requeuedEvent := <-stakingEventReceivedChan:
		require.Nil(t, requeuedEvent, "Event should not be available immediately in the main queue")
	case <-time.After(3 * time.Second): // Wait longer than the delay to ensure the message moves back
	}

	// Now let's wait for the requeued event
	time.Sleep(2 * time.Second) // Wait additional time for delayed message to return
	requeuedEvent := <-stakingEventReceivedChan
	require.NotNil(t, requeuedEvent)
	require.Equal(t, int32(1), requeuedEvent.RetryAttempts)
}
