package processor

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/backoff"
	"github.com/overtonx/outbox/v2/embedded"
	"github.com/overtonx/outbox/v2/internal/metric"
)

type StuckEventServiceImpl struct {
	db                *sql.DB
	logger            *zap.Logger
	backoffStrategy   backoff.BackoffStrategy
	maxAttempts       int
	batchSize         int
	stuckEventTimeout time.Duration
	metrics           embedded.MetricsCollector
}

func NewStuckEventService(
	db *sql.DB,
	logger *zap.Logger,
	backoffStrategy backoff.BackoffStrategy,
	maxAttempts int,
	batchSize int,
	stuckEventTimeout time.Duration,
	metrics embedded.MetricsCollector,
) *StuckEventServiceImpl {
	if metrics == nil {
		metrics = metric.NewNoOpMetricsCollector()
	}

	return &StuckEventServiceImpl{
		db:                db,
		logger:            logger,
		backoffStrategy:   backoffStrategy,
		maxAttempts:       maxAttempts,
		batchSize:         batchSize,
		stuckEventTimeout: stuckEventTimeout,
		metrics:           metrics,
	}
}

func (s *StuckEventServiceImpl) RecoverStuckEvents(ctx context.Context) error {
	start := time.Now()
	defer func() {
		s.metrics.RecordDuration("stuck_events.recovery.duration", time.Since(start), nil)
	}()

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
	})
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	timeoutThreshold := time.Now().Add(-s.stuckEventTimeout)

	query := `
		SELECT id, event_id, attempt_count, updated_at
		FROM outbox_events 
		WHERE status = ? AND updated_at < ?
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query, embedded.EventRecordStatusProcessing, timeoutThreshold, s.batchSize)
	if err != nil {
		return fmt.Errorf("failed to query stuck events: %w", err)
	}
	defer rows.Close()

	var stuckEvents []struct {
		ID           int64
		EventID      string
		AttemptCount int
		UpdatedAt    time.Time
	}

	for rows.Next() {
		var event struct {
			ID           int64
			EventID      string
			AttemptCount int
			UpdatedAt    time.Time
		}

		err := rows.Scan(&event.ID, &event.EventID, &event.AttemptCount, &event.UpdatedAt)
		if err != nil {
			s.logger.Error("Failed to scan stuck event", zap.Error(err))
			continue
		}

		stuckEvents = append(stuckEvents, event)
	}

	if len(stuckEvents) == 0 {
		return nil
	}

	recoveredCount := 0
	for _, event := range stuckEvents {
		var newStatus int
		if event.AttemptCount >= s.maxAttempts {
			newStatus = embedded.EventRecordStatusError
		} else {
			newStatus = embedded.EventRecordStatusRetry
		}

		nextAttempt := s.backoffStrategy.CalculateNextAttempt(event.AttemptCount)

		updateQuery := `
			UPDATE outbox_events 
			SET status = ?, next_attempt_at = ?, updated_at = NOW()
			WHERE id = ?
		`

		_, err := tx.ExecContext(ctx, updateQuery, newStatus, nextAttempt, event.ID)
		if err != nil {
			s.logger.Error("Failed to recover stuck event",
				zap.Error(err),
				zap.String("event_id", event.EventID),
				zap.Int64("id", event.ID))
			continue
		}

		recoveredCount++
		s.logger.Info("Recovered stuck event",
			zap.String("event_id", event.EventID),
			zap.Int64("id", event.ID),
			zap.Int("old_status", embedded.EventRecordStatusProcessing),
			zap.Int("new_status", newStatus),
			zap.Int("attempt_count", event.AttemptCount),
			zap.Time("updated_at", event.UpdatedAt))
	}

	if err = tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit stuck events recovery transaction: %w", err)
	}

	s.logger.Info("Stuck events recovery completed",
		zap.Int("total_found", len(stuckEvents)),
		zap.Int("recovered", recoveredCount),
		zap.Duration("timeout_threshold", s.stuckEventTimeout))

	s.metrics.IncrementCounter("stuck_events.recovered", map[string]string{"status": "success"})
	s.metrics.RecordGauge("stuck_events.batch_size", float64(recoveredCount), nil)

	return nil
}
