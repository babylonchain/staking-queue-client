package queuemngr

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/babylonchain/staking-queue-client/client"
	"github.com/babylonchain/staking-queue-client/config"
)

const timeout = 5 * time.Second

type QueueManager struct {
	StakingQueue       client.QueueClient
	UnbondingQueue     client.QueueClient
	WithdrawQueue      client.QueueClient
	ExpiryQueue        client.QueueClient
	StatsQueue         client.QueueClient
	BtcInfoQueue       client.QueueClient
	ConfirmedInfoQueue client.QueueClient
	logger             *zap.Logger
}

func NewQueueManager(cfg *config.QueueConfig, logger *zap.Logger) (*QueueManager, error) {
	stakingQueue, err := client.NewQueueClient(cfg, client.ActiveStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create staking queue: %w", err)
	}

	unbondingQueue, err := client.NewQueueClient(cfg, client.UnbondingStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create unbonding queue: %w", err)
	}

	withdrawQueue, err := client.NewQueueClient(cfg, client.WithdrawStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create withdraw queue: %w", err)
	}

	expiryQueue, err := client.NewQueueClient(cfg, client.ExpiredStakingQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create withdraw queue: %w", err)
	}

	statsQueue, err := client.NewQueueClient(cfg, client.StakingStatsQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create stats queue: %w", err)
	}

	btcInfoQueue, err := client.NewQueueClient(cfg, client.BtcInfoQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create btc info queue: %w", err)
	}

	confirmedInfoQueue, err := client.NewQueueClient(cfg, client.ConfirmedInfoQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create confirmed info queue: %w", err)
	}

	return &QueueManager{
		StakingQueue:       stakingQueue,
		UnbondingQueue:     unbondingQueue,
		WithdrawQueue:      withdrawQueue,
		ExpiryQueue:        expiryQueue,
		StatsQueue:         statsQueue,
		BtcInfoQueue:       btcInfoQueue,
		ConfirmedInfoQueue: confirmedInfoQueue,
		logger:             logger.With(zap.String("module", "queue consumer")),
	}, nil
}

func (qc *QueueManager) Start() error {
	return nil
}

func (qc *QueueManager) PushStakingEvent(ev *client.ActiveStakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing staking event", zap.String("tx_hash", ev.StakingTxHashHex))
	err = qc.StakingQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push staking event: %w", err)
	}
	qc.logger.Info("successfully pushed staking event", zap.String("tx_hash", ev.StakingTxHashHex))

	return nil
}

func (qc *QueueManager) PushUnbondingEvent(ev *client.UnbondingStakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing unbonding event", zap.String("staking_tx_hash", ev.UnbondingTxHashHex))
	err = qc.UnbondingQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push unbonding event: %w", err)
	}
	qc.logger.Info("successfully pushed unbonding event", zap.String("staking_tx_hash", ev.UnbondingTxHashHex))

	return nil
}

func (qc *QueueManager) PushWithdrawEvent(ev *client.WithdrawStakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing withdraw event", zap.String("staking_tx_hash", ev.StakingTxHashHex))
	err = qc.WithdrawQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push withdraw event: %w", err)
	}
	qc.logger.Info("successfully pushed withdraw event", zap.String("staking_tx_hash", ev.StakingTxHashHex))

	return nil
}

func (qc *QueueManager) PushExpiryEvent(ev *client.ExpiredStakingEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing expiry event", zap.String("tx_hash", ev.StakingTxHashHex))
	err = qc.ExpiryQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push expiry event: %w", err)
	}
	qc.logger.Info("successfully pushed expiry event", zap.String("tx_hash", ev.StakingTxHashHex))

	return nil
}

func (qc *QueueManager) PushBtcInfoEvent(ev *client.BtcInfoEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing btc info event", zap.Uint64("height", ev.Height))
	err = qc.BtcInfoQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push btc info event: %w", err)
	}
	qc.logger.Info("successfully pushed btc info event", zap.Uint64("height", ev.Height))

	return nil
}

func (qc *QueueManager) PushConfirmedInfoEvent(ev *client.ConfirmedInfoEvent) error {
	jsonBytes, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	messageBody := string(jsonBytes)

	qc.logger.Info("pushing confirmed info event",
		zap.Uint64("height", ev.Height),
		zap.Uint64("tvl", ev.Tvl),
	)
	err = qc.ConfirmedInfoQueue.SendMessage(context.TODO(), messageBody)
	if err != nil {
		return fmt.Errorf("failed to push confirmed info event: %w", err)
	}
	qc.logger.Info("successfully pushed confirmed info event", zap.Uint64("height", ev.Height))

	return nil
}

// requeue message
func (qc *QueueManager) ReQueueMessage(ctx context.Context, message client.QueueMessage, queueName string) error {
	switch queueName {
	case client.ActiveStakingQueueName:
		return qc.StakingQueue.ReQueueMessage(ctx, message)
	case client.UnbondingStakingQueueName:
		return qc.UnbondingQueue.ReQueueMessage(ctx, message)
	case client.WithdrawStakingQueueName:
		return qc.WithdrawQueue.ReQueueMessage(ctx, message)
	case client.ExpiredStakingQueueName:
		return qc.ExpiryQueue.ReQueueMessage(ctx, message)
	case client.StakingStatsQueueName:
		return qc.StatsQueue.ReQueueMessage(ctx, message)
	case client.BtcInfoQueueName:
		return qc.BtcInfoQueue.ReQueueMessage(ctx, message)
	case client.ConfirmedInfoQueueName:
		return qc.ConfirmedInfoQueue.ReQueueMessage(ctx, message)
	default:
		return fmt.Errorf("unknown queue name: %s", queueName)
	}
}

func (qc *QueueManager) Stop() error {
	if err := qc.StakingQueue.Stop(); err != nil {
		return err
	}

	if err := qc.UnbondingQueue.Stop(); err != nil {
		return err
	}

	if err := qc.WithdrawQueue.Stop(); err != nil {
		return err
	}

	if err := qc.ExpiryQueue.Stop(); err != nil {
		return err
	}

	if err := qc.StatsQueue.Stop(); err != nil {
		return err
	}

	if err := qc.BtcInfoQueue.Stop(); err != nil {
		return err
	}

	if err := qc.ConfirmedInfoQueue.Stop(); err != nil {
		return err
	}

	return nil
}

// Ping checks the health of the RabbitMQ infrastructure.
func (qc *QueueManager) Ping() error {
	queues := []client.QueueClient{
		qc.StakingQueue,
		qc.UnbondingQueue,
		qc.WithdrawQueue,
		qc.ExpiryQueue,
		qc.StatsQueue,
		qc.BtcInfoQueue,
		qc.ConfirmedInfoQueue,
	}

	for _, queue := range queues {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		err := queue.Ping(ctx)
		if err != nil {
			qc.logger.Error("ping failed", zap.String("queue", queue.GetQueueName()), zap.Error(err))
			return err
		}
		qc.logger.Info("ping successful", zap.String("queue", queue.GetQueueName()))
	}
	return nil
}
