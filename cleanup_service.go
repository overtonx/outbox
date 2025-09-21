package outbox

import (
	"context"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/storage"
)

// CleanupServiceImpl выполняет очистку старых записей.
type CleanupServiceImpl struct {
	store               storage.Store
	logger              *zap.Logger
	metrics             MetricsCollector
	sentEventRetention  time.Duration
	deadLetterRetention time.Duration
}

// NewCleanupService создает новый экземпляр CleanupServiceImpl.
func NewCleanupService(
	store storage.Store,
	logger *zap.Logger,
	metrics MetricsCollector,
	sentEventRetention time.Duration,
	deadLetterRetention time.Duration,
) *CleanupServiceImpl {
	if metrics == nil {
		metrics = NewNoOpMetricsCollector()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &CleanupServiceImpl{
		store:               store,
		logger:              logger,
		metrics:             metrics,
		sentEventRetention:  sentEventRetention,
		deadLetterRetention: deadLetterRetention,
	}
}

// Cleanup - это workFunc для воркера, который выполняет очистку.
func (s *CleanupServiceImpl) Cleanup(ctx context.Context) error {
	start := time.Now()
	defer func() {
		s.metrics.RecordDuration("cleanup.duration", time.Since(start), nil)
	}()

	s.logger.Info("Starting cleanup process")

	// Очистка успешно отправленных событий
	sentDeleted, err := s.store.DeleteSentEvents(ctx, s.sentEventRetention)
	if err != nil {
		s.logger.Error("Failed to clean up sent events", zap.Error(err))
		s.metrics.IncrementCounter("cleanup.sent_events.failed", nil)
	} else if sentDeleted > 0 {
		s.logger.Info("Cleaned up sent events", zap.Int64("count", sentDeleted))
		s.metrics.RecordGauge("cleanup.sent_events.deleted", float64(sentDeleted), nil)
	}

	// Очистка событий из DLQ
	dlDeleted, err := s.store.DeleteDeadLetterEvents(ctx, s.deadLetterRetention)
	if err != nil {
		s.logger.Error("Failed to clean up dead-letter events", zap.Error(err))
		s.metrics.IncrementCounter("cleanup.dead_letter.failed", nil)
	} else if dlDeleted > 0 {
		s.logger.Info("Cleaned up dead-letter events", zap.Int64("count", dlDeleted))
		s.metrics.RecordGauge("cleanup.dead_letter.deleted", float64(dlDeleted), nil)
	}

	s.logger.Info("Cleanup process finished")
	s.metrics.IncrementCounter("cleanup.executed", nil)

	// В данной реализации воркер очистки всегда возвращает nil,
	// чтобы не останавливать его работу из-за ошибок очистки.
	return nil
}
