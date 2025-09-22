package processor

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/embedded"
	"github.com/overtonx/outbox/v2/internal/metric"
)

type CleanupServiceImpl struct {
	db                  *sql.DB
	logger              *zap.Logger
	batchSize           int
	deadLetterRetention time.Duration
	sentEventsRetention time.Duration
	metrics             embedded.MetricsCollector
}

func NewCleanupService(
	db *sql.DB,
	logger *zap.Logger,
	batchSize int,
	deadLetterRetention time.Duration,
	sentEventsRetention time.Duration,
	metrics embedded.MetricsCollector,
) *CleanupServiceImpl {
	if metrics == nil {
		metrics = metric.NewNoOpMetricsCollector()
	}

	return &CleanupServiceImpl{
		db:                  db,
		logger:              logger,
		batchSize:           batchSize,
		deadLetterRetention: deadLetterRetention,
		sentEventsRetention: sentEventsRetention,
		metrics:             metrics,
	}
}

func (s *CleanupServiceImpl) Cleanup(ctx context.Context) error {
	start := time.Now()
	defer func() {
		s.metrics.RecordDuration("cleanup.duration", time.Since(start), nil)
	}()

	deadLetterCount, err := s.cleanupDeadLetters(ctx)
	if err != nil {
		s.logger.Error("Failed to cleanup deadletters", zap.Error(err))
	}

	sentEventCount, err := s.cleanupSentEvents(ctx)
	if err != nil {
		s.logger.Error("Failed to cleanup sent events", zap.Error(err))
	}

	totalCleaned := deadLetterCount + sentEventCount
	s.logger.Info("Cleanup completed",
		zap.Int64("deadletters_cleaned", deadLetterCount),
		zap.Int64("sent_events_cleaned", sentEventCount),
		zap.Int64("total_cleaned", totalCleaned))

	s.metrics.IncrementCounter("cleanup.executed", map[string]string{"status": "success"})
	s.metrics.RecordGauge("cleanup.deadletters_cleaned", float64(deadLetterCount), nil)
	s.metrics.RecordGauge("cleanup.sent_events_cleaned", float64(sentEventCount), nil)

	return nil
}

func (s *CleanupServiceImpl) cleanupDeadLetters(ctx context.Context) (int64, error) {
	cutoffTime := time.Now().Add(-s.deadLetterRetention)

	query := `
		DELETE FROM outbox_deadletters 
		WHERE created_at < ?
		LIMIT ?
	`

	result, err := s.db.ExecContext(ctx, query, cutoffTime, s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup deadletters: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected > 0 {
		s.logger.Info("Cleaned up old deadletters",
			zap.Int64("deleted_count", rowsAffected),
			zap.Time("cutoff_time", cutoffTime),
			zap.Duration("retention_period", s.deadLetterRetention))
	}

	return rowsAffected, nil
}

func (s *CleanupServiceImpl) cleanupSentEvents(ctx context.Context) (int64, error) {
	cutoffTime := time.Now().Add(-s.sentEventsRetention)

	query := `
		DELETE FROM outbox_events 
		WHERE status = ? AND created_at < ?
		LIMIT ?
	`

	result, err := s.db.ExecContext(ctx, query, embedded.EventRecordStatusSent, cutoffTime, s.batchSize)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup sent events: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected > 0 {
		s.logger.Info("Cleaned up old sent events",
			zap.Int64("deleted_count", rowsAffected),
			zap.Time("cutoff_time", cutoffTime),
			zap.Duration("retention_period", s.sentEventsRetention))
	}

	return rowsAffected, nil
}
