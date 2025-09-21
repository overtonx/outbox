package outbox

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/storage"
)

// DeadLetterServiceImpl обрабатывает события, которые не удалось доставить.
type DeadLetterServiceImpl struct {
	store       storage.Store
	logger      *zap.Logger
	metrics     MetricsCollector
	batchSize   int
	maxAttempts int
}

// NewDeadLetterService создает новый экземпляр DeadLetterServiceImpl.
func NewDeadLetterService(
	store storage.Store,
	logger *zap.Logger,
	metrics MetricsCollector,
	batchSize int,
	maxAttempts int,
) *DeadLetterServiceImpl {
	if metrics == nil {
		metrics = NewNoOpMetricsCollector()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &DeadLetterServiceImpl{
		store:       store,
		logger:      logger,
		metrics:     metrics,
		batchSize:   batchSize,
		maxAttempts: maxAttempts,
	}
}

// MoveToDeadLetters - это workFunc для воркера, который перемещает события в DLQ.
func (s *DeadLetterServiceImpl) MoveToDeadLetters(ctx context.Context) error {
	start := time.Now()
	defer func() {
		s.metrics.RecordDuration("deadletter.duration", time.Since(start), nil)
	}()

	events, err := s.store.FetchEventsToMoveToDeadLetter(ctx, s.batchSize, s.maxAttempts)
	if err != nil {
		return fmt.Errorf("failed to fetch events for dead-letter queue: %w", err)
	}

	if len(events) == 0 {
		return nil
	}

	s.logger.Info("Found events to move to dead-letter queue", zap.Int("count", len(events)))
	s.metrics.RecordGauge("deadletter.batch_size", float64(len(events)), nil)

	movedCount := 0
	for _, event := range events {
		select {
		case <-ctx.Done():
			s.logger.Warn("Context cancelled during dead-letter processing", zap.Error(ctx.Err()))
			return ctx.Err()
		default:
		}

		// В `event` из `FetchEventsToMoveToDeadLetter` не хватает `last_error`.
		// Это поле должно быть добавлено в `scanEvents` и `EventRecord`.
		// Пока что передаем пустую строку.
		// TODO: Исправить `FetchEventsToMoveToDeadLetter`, чтобы он возвращал `last_error`.
		lastError := "unknown error"

		if err := s.store.MoveToDeadLetter(ctx, event, lastError); err != nil {
			s.logger.Error("Failed to move event to dead-letter queue",
				zap.Int64("event_id", event.ID),
				zap.Error(err),
			)
			s.metrics.IncrementCounter("deadletter.move_failed", nil)
			continue
		}
		movedCount++
		s.metrics.IncrementCounter("deadletter.move_success", nil)
	}

	s.logger.Info("Finished moving events to dead-letter queue", zap.Int("moved_count", movedCount))
	return nil
}
