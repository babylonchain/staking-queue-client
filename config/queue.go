package config

import (
	"fmt"
	"time"
)

const (
	defaultQueueUser                = "user"
	defaultQueuePassword            = "password"
	defaultQueueUrl                 = "localhost:5672"
	defaultQueueProcessingTimeout   = 5
	defaultQueueMsgMaxRetryAttempts = 10
	defaultReQueueDelayTime         = 5
)

type QueueConfig struct {
	QueueUser              string        `mapstructure:"queue_user"`
	QueuePassword          string        `mapstructure:"queue_password"`
	Url                    string        `mapstructure:"url"`
	QueueProcessingTimeout time.Duration `mapstructure:"processing_timeout"`
	MsgMaxRetryAttempts    int32         `mapstructure:"msg_max_retry_attempts"`
	ReQueueDelayTime       time.Duration `mapstructure:"requeue_delay_time"`
}

func (cfg *QueueConfig) Validate() error {
	if cfg.QueueUser == "" {
		return fmt.Errorf("missing queue user")
	}

	if cfg.QueuePassword == "" {
		return fmt.Errorf("missing queue password")
	}

	if cfg.Url == "" {
		return fmt.Errorf("missing queue url")
	}

	if cfg.QueueProcessingTimeout <= 0 {
		return fmt.Errorf("invalid queue processing timeout")
	}

	if cfg.MsgMaxRetryAttempts <= 0 {
		return fmt.Errorf("invalid queue message max retry attempts")
	}

	if cfg.ReQueueDelayTime <= 0 {
		return fmt.Errorf(`invalid requeue delay time. 
		It should be greater than 0, the unit is seconds`)
	}

	return nil
}

func DefaultQueueConfig() *QueueConfig {
	return &QueueConfig{
		QueueUser:              defaultQueueUser,
		QueuePassword:          defaultQueuePassword,
		Url:                    defaultQueueUrl,
		QueueProcessingTimeout: defaultQueueProcessingTimeout,
		MsgMaxRetryAttempts:    defaultQueueMsgMaxRetryAttempts,
		ReQueueDelayTime:       defaultReQueueDelayTime,
	}
}
