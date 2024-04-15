package client

import "fmt"

const (
	ActiveStakingQueueName    string = "active_staking_queue"
	UnbondingStakingQueueName string = "unbonding_staking_queue"
	WithdrawStakingQueueName  string = "withdraw_staking_queue"
	ExpiredStakingQueueName   string = "expired_staking_queue"
)

const (
	ActiveStakingEventType    EventType = 1
	UnbondingStakingEventType EventType = 2
	WithdrawStakingEventType  EventType = 3
	ExpiredStakingEventType   EventType = 4
	StakingStatsEventType     EventType = 5
)

type EventType int

type EventMessage interface {
	GetEventType() EventType
	GetStakingTxHashHex() string
}

type ActiveStakingEvent struct {
	EventType             EventType `json:"event_type"` // always 1. ActiveStakingEventType
	StakingTxHashHex      string    `json:"staking_tx_hash_hex"`
	StakerPkHex           string    `json:"staker_pk_hex"`
	FinalityProviderPkHex string    `json:"finality_provider_pk_hex"`
	StakingValue          uint64    `json:"staking_value"`
	StakingStartHeight    uint64    `json:"staking_start_height"`
	StakingStartTimestamp int64     `json:"staking_start_timestamp"`
	StakingTimeLock       uint64    `json:"staking_timelock"`
	StakingOutputIndex    uint64    `json:"staking_output_index"`
	StakingTxHex          string    `json:"staking_tx_hex"`
}

func (e ActiveStakingEvent) GetEventType() EventType {
	return ActiveStakingEventType
}

func (e ActiveStakingEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewActiveStakingEvent(
	stakingTxHashHex string,
	stakerPkHex string,
	finalityProviderPkHex string,
	stakingValue uint64,
	stakingStartHeight uint64,
	stakingStartTimestamp int64,
	stakingTimeLock uint64,
	stakingOutputIndex uint64,
	stakingTxHex string,
) ActiveStakingEvent {
	return ActiveStakingEvent{
		EventType:             ActiveStakingEventType,
		StakingTxHashHex:      stakingTxHashHex,
		StakerPkHex:           stakerPkHex,
		FinalityProviderPkHex: finalityProviderPkHex,
		StakingValue:          stakingValue,
		StakingStartHeight:    stakingStartHeight,
		StakingStartTimestamp: stakingStartTimestamp,
		StakingTimeLock:       stakingTimeLock,
		StakingOutputIndex:    stakingOutputIndex,
		StakingTxHex:          stakingTxHex,
	}
}

type UnbondingStakingEvent struct {
	EventType               EventType `json:"event_type"` // always 2. UnbondingStakingEventType
	StakingTxHashHex        string    `json:"staking_tx_hash_hex"`
	UnbondingStartHeight    uint64    `json:"unbonding_start_height"`
	UnbondingStartTimestamp int64     `json:"unbonding_start_timestamp"`
	UnbondingTimeLock       uint64    `json:"unbonding_timelock"`
	UnbondingOutputIndex    uint64    `json:"unbonding_output_index"`
	UnbondingTxHex          string    `json:"unbonding_tx_hex"`
	UnbondingTxHashHex      string    `json:"unbonding_tx_hash_hex"`
}

func (e UnbondingStakingEvent) GetEventType() EventType {
	return UnbondingStakingEventType
}

func (e UnbondingStakingEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewUnbondingStakingEvent(
	stakingTxHashHex string,
	unbondingStartHeight uint64,
	unbondingStartTimestamp int64,
	unbondingTimeLock uint64,
	unbondingOutputIndex uint64,
	unbondingTxHex string,
	unbondingTxHashHex string,
) UnbondingStakingEvent {
	return UnbondingStakingEvent{
		EventType:               UnbondingStakingEventType,
		StakingTxHashHex:        stakingTxHashHex,
		UnbondingStartHeight:    unbondingStartHeight,
		UnbondingStartTimestamp: unbondingStartTimestamp,
		UnbondingTimeLock:       unbondingTimeLock,
		UnbondingOutputIndex:    unbondingOutputIndex,
		UnbondingTxHex:          unbondingTxHex,
		UnbondingTxHashHex:      unbondingTxHashHex,
	}
}

type WithdrawStakingEvent struct {
	EventType        EventType `json:"event_type"` // always 3. WithdrawStakingEventType
	StakingTxHashHex string    `json:"staking_tx_hash_hex"`
}

func (e WithdrawStakingEvent) GetEventType() EventType {
	return WithdrawStakingEventType
}

func (e WithdrawStakingEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewWithdrawStakingEvent(stakingTxHashHex string) WithdrawStakingEvent {
	return WithdrawStakingEvent{
		EventType:        WithdrawStakingEventType,
		StakingTxHashHex: stakingTxHashHex,
	}
}

type StakingTxType string

const (
	ActiveTxType    StakingTxType = "active"
	UnbondingTxType StakingTxType = "unbonding"
)

func (s StakingTxType) ToString() string {
	return string(s)
}

func FromStringToStakingTxType(s string) (StakingTxType, error) {
	switch s {
	case ActiveTxType.ToString():
		return ActiveTxType, nil
	case UnbondingTxType.ToString():
		return UnbondingTxType, nil
	default:
		return "", fmt.Errorf("invalid staking transaction type: %s", s)
	}
}

type ExpiredStakingEvent struct {
	EventType        EventType     `json:"event_type"` // always 4. ExpiredStakingEventType
	StakingTxHashHex string        `json:"staking_tx_hash_hex"`
	TxType           StakingTxType `json:"tx_type"`
}

func (e ExpiredStakingEvent) GetEventType() EventType {
	return ExpiredStakingEventType
}

func (e ExpiredStakingEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewExpiredStakingEvent(stakingTxHashHex string, txType StakingTxType) ExpiredStakingEvent {
	return ExpiredStakingEvent{
		EventType:        ExpiredStakingEventType,
		StakingTxHashHex: stakingTxHashHex,
		TxType:           txType,
	}
}

type StakingStatsEvent struct {
	EventType             EventType     `json:"event_type"` // always 5. StakingStatsEventType
	StakingTxHashHex      string        `json:"staking_tx_hash_hex"`
	TxType                StakingTxType `json:"tx_type"`
	StakingValue          uint64        `json:"staking_value"`
	StakerPkHex           string        `json:"staker_pk_hex"`
	FinalityProviderPkHex string        `json:"finality_provider_pk_hex"`
}

func (e StakingStatsEvent) GetEventType() EventType {
	return StakingStatsEventType
}

func (e StakingStatsEvent) GetStakingTxHashHex() string {
	return e.StakingTxHashHex
}

func NewStakingStatsEvent(
	stakingTxHashHex string,
	txType StakingTxType,
	stakingValue uint64,
	stakerPkHex string,
	finalityProviderPkHex string,
) StakingStatsEvent {
	return StakingStatsEvent{
		EventType:             StakingStatsEventType,
		StakingTxHashHex:      stakingTxHashHex,
		TxType:                txType,
		StakingValue:          stakingValue,
		StakerPkHex:           stakerPkHex,
		FinalityProviderPkHex: finalityProviderPkHex,
	}
}
