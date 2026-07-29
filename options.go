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

	// defaultInFlightWindow — для OrderedPublisher (см. publisher.go):
	// сколько событий вперёд от чекпоинта можно опубликовать, не дожидаясь
	// ack'ов, прежде чем claim следующей порции приостановится. Ограничивает
	// объём переотправки после Recreate/replay и рост checkpointTracker.
	defaultInFlightWindow = 1000
	// defaultCheckpointFlushInterval — как часто персистентный чекпоинт
	// (outbox_checkpoints) и построчный status обновляются из watermark для
	// OrderedPublisher — не на каждый ack (см. checkpoint.go).
	defaultCheckpointFlushInterval = 200 * time.Millisecond
	// defaultPoisonMessageThreshold — сколько подряд идущих циклов
	// stop→Recreate→replay должны упереться в одно и то же событие (id сразу
	// после watermark), прежде чем оно будет принудительно перенесено в
	// outbox_deadletters, а чекпоинт продвинут мимо него. Это единственное
	// исключение из "никаких точечных ретраев" — без него poison-сообщение
	// останавливало бы весь упорядоченный поток навсегда.
	defaultPoisonMessageThreshold = 3
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

	inFlightWindow          int
	checkpointFlushInterval time.Duration
	poisonMessageThreshold  int
}

func defaultDispatcherOptions() *dispatcherOptions {
	return &dispatcherOptions{
		batchSize:               defaultBatchSize,
		pollInterval:            defaultPollInterval,
		maxAttempts:             defaultMaxAttempts,
		processingLeaseTimeout:  defaultProcessingLeaseTimeout,
		backoffStrategy:         DefaultBackoffStrategy(),
		metrics:                 NewOTelMetrics(),
		logger:                  zap.NewNop(),
		inFlightWindow:          defaultInFlightWindow,
		checkpointFlushInterval: defaultCheckpointFlushInterval,
		poisonMessageThreshold:  defaultPoisonMessageThreshold,
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

// WithInFlightWindow задаёт, для Publisher'ов, реализующих OrderedPublisher
// (см. publisher.go), сколько событий вперёд от последнего подтверждённого
// чекпоинта можно опубликовать, не дожидаясь ack'ов. Не влияет на Publisher'ы
// без поддержки чекпоинта (используется прежняя построчная модель).
func WithInFlightWindow(window int) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if window <= 0 {
			return fmt.Errorf("in-flight window must be positive, got %d", window)
		}
		opts.inFlightWindow = window
		return nil
	}
}

// WithCheckpointFlushInterval задаёт, как часто персистентный чекпоинт
// (outbox_checkpoints) и построчный status обновляются из watermark —
// периодически, а не на каждый ack. Применимо только к OrderedPublisher.
func WithCheckpointFlushInterval(interval time.Duration) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if interval <= 0 {
			return fmt.Errorf("checkpoint flush interval must be positive, got %s", interval)
		}
		opts.checkpointFlushInterval = interval
		return nil
	}
}

// WithPoisonMessageThreshold задаёт число подряд идущих циклов
// stop→Recreate→replay, упирающихся в одно и то же событие сразу после
// чекпоинта, после которого оно принудительно переносится в
// outbox_deadletters, а чекпоинт продвигается мимо него. Применимо только к
// OrderedPublisher.
func WithPoisonMessageThreshold(threshold int) DispatcherOption {
	return func(opts *dispatcherOptions) error {
		if threshold <= 0 {
			return fmt.Errorf("poison message threshold must be positive, got %d", threshold)
		}
		opts.poisonMessageThreshold = threshold
		return nil
	}
}
