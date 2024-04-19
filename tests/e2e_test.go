package tests

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/babylonchain/staking-queue-client/client"
	"github.com/babylonchain/staking-queue-client/config"
)

const (
	mockStakerHash = "0x1234567890abcdef"
)

func TestStakingEvent(t *testing.T) {
	numStakingEvents := 3
	activeStakingEvents := buildActiveNStakingEvents(mockStakerHash, numStakingEvents)
	queueCfg := config.DefaultQueueConfig()

	queueManager := setupTestQueueConsumer(t, queueCfg)
	defer func() {
		err := queueManager.Stop()
		require.NoError(t, err)
	}()

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

	queueManager := setupTestQueueConsumer(t, queueCfg)
	defer func() {
		err := queueManager.Stop()
		require.NoError(t, err)
	}()

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

	queueManager := setupTestQueueConsumer(t, queueCfg)
	defer func() {
		err := queueManager.Stop()
		require.NoError(t, err)
	}()

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

	queueManager := setupTestQueueConsumer(t, queueCfg)
	defer func() {
		err := queueManager.Stop()
		require.NoError(t, err)
	}()

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
