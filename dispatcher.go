package outbox

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/overtonx/outbox/v4/migrations"
	"go.uber.org/zap"
)

// Dispatcher читает неотправленные события из outbox_events и публикует их
// в Kafka. При нескольких работающих инстансах Dispatcher (например,
// несколько подов одного сервиса) активно клеймит и публикует ровно один
// лидер, выбранный через MySQL advisory-лок (см. leader.go) — это гарантирует
// глобальный порядок публикации. Каждый тик лидер выполняет один
// claim+publish цикл (claim.go/publish.go), затем переносит события,
// исчерпавшие попытки, в outbox_deadletters (deadletter.go).
type Dispatcher struct {
	db      *sql.DB
	elector *leaderElector

	publisher       Publisher
	metrics         MetricsCollector
	backoffStrategy BackoffStrategy
	logger          *zap.Logger

	batchSize              int
	pollInterval           time.Duration
	maxAttempts            int
	processingLeaseTimeout time.Duration
	lockName               string

	mu      sync.RWMutex
	started bool
	leading bool
	cancel  context.CancelFunc
}

// NewDispatcher создаёт Dispatcher, применяет встроенные миграции схемы
// outbox (migrations.Migrate) и настраивает Kafka-публикацию по умолчанию,
// если Publisher не передан явно через WithPublisher/WithKafkaConfig.
func NewDispatcher(db *sql.DB, opts ...DispatcherOption) (*Dispatcher, error) {
	options := defaultDispatcherOptions()

	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	if options.publisher == nil {
		var err error
		options.publisher, err = NewKafkaPublisher(options.logger)
		if err != nil {
			return nil, err
		}
	}

	ctx := context.Background()

	if err := migrations.Migrate(ctx, db); err != nil {
		return nil, fmt.Errorf("failed to apply outbox migrations: %w", err)
	}

	lockName := options.lockName
	if lockName == "" {
		var err error
		lockName, err = defaultLockName(ctx, db)
		if err != nil {
			return nil, fmt.Errorf("failed to determine leader lock name: %w", err)
		}
	}

	return &Dispatcher{
		db:                     db,
		elector:                newLeaderElector(db, lockName, options.logger),
		publisher:              options.publisher,
		metrics:                options.metrics,
		backoffStrategy:        options.backoffStrategy,
		logger:                 options.logger,
		batchSize:              options.batchSize,
		pollInterval:           options.pollInterval,
		maxAttempts:            options.maxAttempts,
		processingLeaseTimeout: options.processingLeaseTimeout,
		lockName:               lockName,
	}, nil
}

// Start блокируется до отмены ctx или вызова Stop(). Внутри — цикл выбора
// лидера (leader.go); пока текущий процесс лидер, каждые pollInterval
// выполняется reconcile-тик (claim → publish → move dead letters).
func (d *Dispatcher) Start(ctx context.Context) error {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		return fmt.Errorf("outbox: dispatcher already started")
	}
	d.started = true
	runCtx, cancel := context.WithCancel(ctx)
	d.cancel = cancel
	d.mu.Unlock()

	d.logger.Info("outbox: starting dispatcher",
		zap.Int("batch_size", d.batchSize),
		zap.Duration("poll_interval", d.pollInterval),
		zap.Int("max_attempts", d.maxAttempts),
		zap.Duration("processing_lease_timeout", d.processingLeaseTimeout),
		zap.String("lock_name", d.lockName),
	)

	err := d.elector.run(runCtx, d.pollInterval, d.runAsLeader)

	if closeErr := d.publisher.Close(); closeErr != nil {
		d.logger.Error("outbox: failed to close publisher", zap.Error(closeErr))
	}

	d.mu.Lock()
	d.started = false
	d.mu.Unlock()

	if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

// Stop инициирует штатную остановку Dispatcher; Start вернётся после того,
// как текущий reconcile-тик (если он выполняется) завершится и лидерский
// лок будет освобождён.
func (d *Dispatcher) Stop() {
	d.mu.RLock()
	cancel := d.cancel
	d.mu.RUnlock()

	if cancel != nil {
		d.logger.Info("outbox: stopping dispatcher...")
		cancel()
	}
}

// IsLeader сообщает, является ли текущий процесс активным лидером прямо
// сейчас (то есть именно он клеймит и публикует события).
func (d *Dispatcher) IsLeader() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.leading
}

func (d *Dispatcher) setLeading(v bool) {
	d.mu.Lock()
	d.leading = v
	d.mu.Unlock()
}

// runAsLeader выполняется, пока текущий процесс держит лидерский лок:
// немедленный reconcile-тик при получении лидерства, затем один тик на
// каждый pollInterval, пока leaderCtx не будет отменён.
func (d *Dispatcher) runAsLeader(ctx context.Context) {
	d.setLeading(true)
	defer d.setLeading(false)

	d.reconcileOnce(ctx)

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			d.reconcileOnce(ctx)
		}
	}
}

func (d *Dispatcher) reconcileOnce(ctx context.Context) {
	events, err := d.claimBatch(ctx)
	if err != nil {
		d.logger.Error("outbox: failed to claim events", zap.Error(err))
		return
	}

	d.publishBatch(ctx, events)

	if err := d.moveDeadLetters(ctx); err != nil {
		d.logger.Error("outbox: failed to move exhausted events to deadletters", zap.Error(err))
	}
}
