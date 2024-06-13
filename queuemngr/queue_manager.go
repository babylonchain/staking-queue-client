package queuemngr

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"

	"github.com/babylonchain/staking-queue-client/client"
	"github.com/babylonchain/staking-queue-client/config"
)

type QueueManager struct {
	StakingQueue   client.QueueClient
	UnbondingQueue client.QueueClient
	WithdrawQueue  client.QueueClient
	ExpiryQueue    client.QueueClient
	StatsQueue     client.QueueClient
	BtcInfoQueue   client.QueueClient
	logger         *zap.Logger
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

	BtcInfoQueue, err := client.NewQueueClient(cfg, client.BtcInfoQueueName)
	if err != nil {
		return nil, fmt.Errorf("failed to create btc info queue: %w", err)
	}

	return &QueueManager{
		StakingQueue:   stakingQueue,
		UnbondingQueue: unbondingQueue,
		WithdrawQueue:  withdrawQueue,
		ExpiryQueue:    expiryQueue,
		StatsQueue:     statsQueue,
		BtcInfoQueue:   BtcInfoQueue,
		logger:         logger.With(zap.String("module", "queue consumer")),
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

	return nil
}


// Ping checks the health of the RabbitMQ infrastructure.
func (qc *QueueManager) Ping() error {
	queues := map[string]client.QueueClient{
		client.ActiveStakingQueueName:   qc.StakingQueue,
		client.UnbondingStakingQueueName: qc.UnbondingQueue,
		client.WithdrawStakingQueueName:  qc.WithdrawQueue,
		client.ExpiredStakingQueueName:    qc.ExpiryQueue,
		client.StakingStatsQueueName:     qc.StatsQueue,
		client.BtcInfoQueueName:   qc.BtcInfoQueue,
	}

	for name, queue := range queues {
		if err := queue.Ping(); err != nil {
			return fmt.Errorf("ping failed for %s: %w", name, err)
		}
	}

	return nil
}