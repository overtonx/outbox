package outbox

import (
	"fmt"
	"time"

	"go.uber.org/zap"
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
	backoffStrategy         BackoffStrategy
	publisher               Publisher
	metrics                 MetricsCollector
	logger                  *zap.Logger
}

func WithBatchSize(size int) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if size <= 0 {
			return fmt.Errorf("batch size must be positive, got %d", size)
		}
		if size > 10_000 {
			return fmt.Errorf("batch size must not exceed 10000, got %d", size)
		}
		opts.batchSize = size
		return nil
	}
}

func WithPollInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if interval <= 0 {
			return fmt.Errorf("poll interval must be positive, got %s", interval)
		}
		opts.pollInterval = interval
		return nil
	}
}

func WithMaxAttempts(attempts int) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if attempts <= 0 {
			return fmt.Errorf("max attempts must be positive, got %d", attempts)
		}
		opts.maxAttempts = attempts
		return nil
	}
}

func WithDeadLetterInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if interval <= 0 {
			return fmt.Errorf("dead letter interval must be positive, got %s", interval)
		}
		opts.deadLetterInterval = interval
		return nil
	}
}

func WithStuckEventTimeout(timeout time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("stuck event timeout must be positive, got %s", timeout)
		}
		opts.stuckEventTimeout = timeout
		return nil
	}
}

func WithStuckEventCheckInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if interval <= 0 {
			return fmt.Errorf("stuck event check interval must be positive, got %s", interval)
		}
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

func WithBackoffStrategy(strategy BackoffStrategy) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.backoffStrategy = strategy
		return nil
	}
}

func WithPublisher(publisher Publisher) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		opts.publisher = publisher
		return nil
	}
}

func WithMetrics(metrics MetricsCollector) DispatcherOption {
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
