package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/backoff"
	"github.com/overtonx/outbox/v2/embedded"
	"github.com/overtonx/outbox/v2/internal/metric"
	"github.com/overtonx/outbox/v2/internal/processor"
	"github.com/overtonx/outbox/v2/internal/worker"
)

const (
	defaultBatchSize               = 100
	defaultPollInterval            = 2 * time.Second
	defaultMaxAttempts             = 3
	defaultDeadLetterInterval      = 5 * time.Minute
	defaultStuckEventTimeout       = 10 * time.Minute
	defaultStuckEventCheckInterval = 2 * time.Minute
	defaultDeadLetterRetention     = 7 * 24 * time.Hour
	defaultSentEventsRetention     = 24 * time.Hour
	defaultCleanupInterval         = 1 * time.Hour
)

type Dispatcher struct {
	eventProcessor    embedded.EventProcessor
	deadLetterService embedded.DeadLetterService
	stuckEventService embedded.StuckEventService
	cleanupService    embedded.CleanupService
	publisher         embedded.Publisher
	metrics           embedded.MetricsCollector
	logger            *zap.Logger

	workers                 []embedded.Worker
	batchSize               int
	pollInterval            time.Duration
	maxAttempts             int
	deadLetterInterval      time.Duration
	stuckEventTimeout       time.Duration
	stuckEventCheckInterval time.Duration
	deadLetterRetention     time.Duration
	sentEventsRetention     time.Duration
	cleanupInterval         time.Duration

	mu       sync.RWMutex
	started  bool
	stopChan chan struct{}
}

func NewDispatcher(db *sql.DB, opts ...DispatcherOption) (*Dispatcher, error) {
	options := &dispatcherOptions{
		batchSize:               defaultBatchSize,
		pollInterval:            defaultPollInterval,
		maxAttempts:             defaultMaxAttempts,
		deadLetterInterval:      defaultDeadLetterInterval,
		stuckEventTimeout:       defaultStuckEventTimeout,
		stuckEventCheckInterval: defaultStuckEventCheckInterval,
		deadLetterRetention:     defaultDeadLetterRetention,
		sentEventsRetention:     defaultSentEventsRetention,
		cleanupInterval:         defaultCleanupInterval,
		backoffStrategy:         backoff.DefaultBackoffStrategy(),
		metrics:                 metric.NewOpenTelemetryMetricsCollector(),
		logger:                  zap.NewNop(),
	}

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

	if err := ensureOutboxTable(context.Background(), db); err != nil {
		return nil, fmt.Errorf("failed to create outbox tables: %w", err)
	}

	eventProcessor := processor.NewEventProcessor(
		db,
		options.logger,
		options.backoffStrategy,
		options.maxAttempts,
		options.batchSize,
		options.publisher,
		options.metrics,
	)

	deadLetterService := processor.NewDeadLetterService(
		db,
		options.logger,
		options.batchSize,
		options.metrics,
	)

	stuckEventService := processor.NewStuckEventService(
		db,
		options.logger,
		options.backoffStrategy,
		options.maxAttempts,
		options.batchSize,
		options.stuckEventTimeout,
		options.metrics,
	)

	cleanupService := processor.NewCleanupService(
		db,
		options.logger,
		options.batchSize,
		options.deadLetterRetention,
		options.sentEventsRetention,
		options.metrics,
	)

	workers := []embedded.Worker{
		worker.NewBaseWorker("event_processor", options.pollInterval, options.logger, eventProcessor.ProcessEvents),
		worker.NewBaseWorker("deadletter_processor", options.deadLetterInterval, options.logger, deadLetterService.MoveToDeadLetters),
		worker.NewBaseWorker("stuck_events_processor", options.stuckEventCheckInterval, options.logger, stuckEventService.RecoverStuckEvents),
		worker.NewBaseWorker("cleanup_processor", options.cleanupInterval, options.logger, cleanupService.Cleanup),
	}

	return &Dispatcher{
		eventProcessor:          eventProcessor,
		deadLetterService:       deadLetterService,
		stuckEventService:       stuckEventService,
		cleanupService:          cleanupService,
		publisher:               options.publisher,
		metrics:                 options.metrics,
		logger:                  options.logger,
		workers:                 workers,
		batchSize:               options.batchSize,
		pollInterval:            options.pollInterval,
		maxAttempts:             options.maxAttempts,
		deadLetterInterval:      options.deadLetterInterval,
		stuckEventTimeout:       options.stuckEventTimeout,
		stuckEventCheckInterval: options.stuckEventCheckInterval,
		deadLetterRetention:     options.deadLetterRetention,
		sentEventsRetention:     options.sentEventsRetention,
		cleanupInterval:         options.cleanupInterval,
		stopChan:                make(chan struct{}),
	}, nil
}

func (d *Dispatcher) Start(ctx context.Context) {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		d.logger.Warn("Dispatcher already started")
		return
	}
	d.started = true
	d.mu.Unlock()

	d.logger.Info("Starting outbox dispatcher",
		zap.Int("batch_size", d.batchSize),
		zap.Duration("poll_interval", d.pollInterval),
		zap.Int("max_attempts", d.maxAttempts),
		zap.Duration("deadletter_interval", d.deadLetterInterval),
		zap.Duration("stuck_event_timeout", d.stuckEventTimeout),
		zap.Duration("stuck_event_check_interval", d.stuckEventCheckInterval),
		zap.Duration("deadletter_retention", d.deadLetterRetention),
		zap.Duration("sent_events_retention", d.sentEventsRetention),
		zap.Duration("cleanup_interval", d.cleanupInterval),
	)

	for _, worker := range d.workers {
		go worker.Start(ctx)
	}

	select {
	case <-ctx.Done():
		d.logger.Info("Context cancelled, stopping dispatcher")
	case <-d.stopChan:
		d.logger.Info("Stop signal received, stopping dispatcher")
	}

	for _, worker := range d.workers {
		worker.Stop()
	}

	d.mu.Lock()
	d.started = false
	d.mu.Unlock()
}

func (d *Dispatcher) Stop() {
	d.mu.RLock()
	if !d.started {
		d.mu.RUnlock()
		return
	}
	d.mu.RUnlock()

	d.logger.Info("Stopping outbox dispatcher...")
	close(d.stopChan)
}

func (d *Dispatcher) IsStarted() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.started
}

func (d *Dispatcher) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"started": d.IsStarted(),
		"workers": len(d.workers),
		"config": map[string]interface{}{
			"batch_size":                 d.batchSize,
			"poll_interval":              d.pollInterval,
			"max_attempts":               d.maxAttempts,
			"deadletter_interval":        d.deadLetterInterval,
			"stuck_event_timeout":        d.stuckEventTimeout,
			"stuck_event_check_interval": d.stuckEventCheckInterval,
			"deadletter_retention":       d.deadLetterRetention,
			"sent_events_retention":      d.sentEventsRetention,
			"cleanup_interval":           d.cleanupInterval,
		},
	}
}
