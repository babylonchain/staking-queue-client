package config

import (
	"fmt"
	"time"
)

const (
	defaultQueueUser              = "user"
	defaultQueuePassword          = "password"
	defaultQueueUrl               = "localhost:5672"
	defaultQueueProcessingTimeout = 5 * time.Second
)

type QueueConfig struct {
	QueueUser              string        `mapstructure:"queue_user"`
	QueuePassword          string        `mapstructure:"queue_password"`
	Url                    string        `mapstructure:"url"`
	QueueProcessingTimeout time.Duration `mapstructure:"processing_timeout"`
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

	return nil
}

func DefaultQueueConfig() *QueueConfig {
	return &QueueConfig{
		QueueUser:              defaultQueueUser,
		QueuePassword:          defaultQueuePassword,
		Url:                    defaultQueueUrl,
		QueueProcessingTimeout: defaultQueueProcessingTimeout,
	}
}
