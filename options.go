package outbox

import (
	"fmt"
	"time"

	"go.uber.org/zap"
)

const (
	defaultBatchSize              = 100
	defaultPollInterval           = 2 * time.Second
	defaultMaxAttempts            = 3
	defaultProcessingLeaseTimeout = 60 * time.Second
	defaultBaseDelay              = 1 * time.Minute
	defaultMaxDelay               = 30 * time.Minute
)

type DispatcherOption func(*dispatcherOptions) error

type dispatcherOptions struct {
	batchSize              int
	pollInterval           time.Duration
	maxAttempts            int
	processingLeaseTimeout time.Duration
	lockName               string
	backoffStrategy        BackoffStrategy
	publisher              Publisher
	metrics                MetricsCollector
	logger                 *zap.Logger
}

func defaultDispatcherOptions() *dispatcherOptions {
	return &dispatcherOptions{
		batchSize:              defaultBatchSize,
		pollInterval:           defaultPollInterval,
		maxAttempts:            defaultMaxAttempts,
		processingLeaseTimeout: defaultProcessingLeaseTimeout,
		backoffStrategy:        DefaultBackoffStrategy(),
		metrics:                NewOTelMetrics(),
		logger:                 zap.NewNop(),
	}
}

// WithBatchSize задаёт максимальное число событий, забираемых из БД за один
// reconcile-тик.
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

// WithPollInterval задаёт интервал между reconcile-тиками лидера, а также
// интервал, с которым standby-инстансы пытаются переизбраться в лидеры.
func WithPollInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if interval <= 0 {
			return fmt.Errorf("poll interval must be positive, got %s", interval)
		}
		opts.pollInterval = interval
		return nil
	}
}

// WithMaxAttempts задаёт число попыток публикации перед переносом события в
// outbox_deadletters.
func WithMaxAttempts(attempts int) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if attempts <= 0 {
			return fmt.Errorf("max attempts must be positive, got %d", attempts)
		}
		opts.maxAttempts = attempts
		return nil
	}
}

// WithProcessingLeaseTimeout задаёт время, после которого событие, зависшее
// в статусе processing (например, из-за падения инстанса-лидера между
// клеймингом и обновлением финального статуса), считается протухшим и
// переклеймливается в том же reconcile-запросе, что и новые/retry события.
func WithProcessingLeaseTimeout(timeout time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if timeout <= 0 {
			return fmt.Errorf("processing lease timeout must be positive, got %s", timeout)
		}
		opts.processingLeaseTimeout = timeout
		return nil
	}
}

// WithLockName задаёт имя MySQL advisory-лока (GET_LOCK), используемого для
// выбора единственного активного лидера среди нескольких инстансов
// Dispatcher. По умолчанию выводится из имени текущей схемы БД — задавайте
// явно, если в одной схеме работает несколько независимых outbox-каналов.
func WithLockName(name string) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if name == "" {
			return fmt.Errorf("lock name must not be empty")
		}
		opts.lockName = name
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
