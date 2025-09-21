package outbox

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/storage"
)

// StuckEventServiceImpl обрабатывает "зависшие" события.
type StuckEventServiceImpl struct {
	store           storage.Store
	logger          *zap.Logger
	metrics         MetricsCollector
	backoffStrategy BackoffStrategy
	batchSize       int
	stuckTimeout    time.Duration
}

// NewStuckEventService создает новый экземпляр StuckEventServiceImpl.
func NewStuckEventService(
	store storage.Store,
	logger *zap.Logger,
	metrics MetricsCollector,
	backoffStrategy BackoffStrategy,
	batchSize int,
	stuckTimeout time.Duration,
) *StuckEventServiceImpl {
	if metrics == nil {
		metrics = NewNoOpMetricsCollector()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &StuckEventServiceImpl{
		store:           store,
		logger:          logger,
		metrics:         metrics,
		backoffStrategy: backoffStrategy,
		batchSize:       batchSize,
		stuckTimeout:    stuckTimeout,
	}
}

// RecoverStuckEvents - это workFunc для воркера, который восстанавливает зависшие события.
func (s *StuckEventServiceImpl) RecoverStuckEvents(ctx context.Context) error {
	start := time.Now()
	defer func() {
		s.metrics.RecordDuration("stuck_events.duration", time.Since(start), nil)
	}()

	events, err := s.store.FetchStuckEvents(ctx, s.batchSize, s.stuckTimeout)
	if err != nil {
		return fmt.Errorf("failed to fetch stuck events: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	s.logger.Info("Found stuck events to recover", zap.Int("count", len(events)))
	s.metrics.RecordGauge("stuck_events.batch_size", float64(len(events)), nil)

	eventIDs := make([]int64, len(events))
	for i, event := range events {
		eventIDs[i] = event.ID
	}

	// Для простоты, мы просто сбрасываем их в состояние Retry.
	// Используем backoff от последней попытки.
	// В реальной реализации можно было бы добавить более сложную логику.
	// Здесь мы предполагаем, что у всех событий в пакете примерно одинаковое число попыток.
	nextAttemptAt := s.backoffStrategy.CalculateNextAttempt(events[0].AttemptCount + 1)

	if err := s.store.ResetStuckEvents(ctx, eventIDs, nextAttemptAt); err != nil {
		s.logger.Error("Failed to reset stuck events", zap.Error(err))
		s.metrics.IncrementCounter("stuck_events.reset_failed", nil)
		return err
	}

	s.logger.Info("Successfully reset stuck events", zap.Int("count", len(events)))
	s.metrics.IncrementCounter("stuck_events.reset_success", nil)
	return nil
}
