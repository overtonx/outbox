package outbox

import (
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/backoff"
	"github.com/overtonx/outbox/v2/embedded"
)

type DispatcherOption func(*dispatcherOptions) error

type dispatcherOptions struct {
	batchSize               int
	pollInterval            time.Duration
	maxAttempts             int
	deadLetterInterval      time.Duration
	stuckEventTimeout       time.Duration
	stuckEventCheckInterval time.Duration
	deadLetterRetention     time.Duration
	sentEventsRetention     time.Duration
	cleanupInterval         time.Duration
	backoffStrategy         backoff.BackoffStrategy
	publisher               embedded.Publisher
	metrics                 embedded.MetricsCollector
	logger                  *zap.Logger
}

func WithBatchSize(size int) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.batchSize = size
		return nil
	}
}

func WithPollInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.pollInterval = interval
		return nil
	}
}

func WithMaxAttempts(attempts int) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.maxAttempts = attempts
		return nil
	}
}

func WithDeadLetterInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.deadLetterInterval = interval
		return nil
	}
}

func WithStuckEventTimeout(timeout time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.stuckEventTimeout = timeout
		return nil
	}
}

func WithStuckEventCheckInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.stuckEventCheckInterval = interval
		return nil
	}
}

func WithDeadLetterRetention(retention time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.deadLetterRetention = retention
		return nil
	}
}

func WithSentEventsRetention(retention time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.sentEventsRetention = retention
		return nil
	}
}

func WithCleanupInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.cleanupInterval = interval
		return nil
	}
}

func WithBackoffStrategy(strategy backoff.BackoffStrategy) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.backoffStrategy = strategy
		return nil
	}
}

func WithPublisher(publisher embedded.Publisher) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.publisher = publisher
		return nil
	}
}

func WithMetrics(metrics embedded.MetricsCollector) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.metrics = metrics
		return nil
	}
}

func WithLogger(logger *zap.Logger) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.logger = logger
		return nil
	}
}

func WithKafkaConfig(config KafkaConfig) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		var err error
		opts.publisher, err = NewKafkaPublisherWithConfig(opts.logger, config)
		return err
	}
}
