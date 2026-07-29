package outbox

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

// publishBatch публикует ранее заклеймленные события в Kafka строго
// последовательно, в том порядке, в котором они были прочитаны из БД
// (claimBatch сортирует по created_at, id) — это и есть гарантия
// сохранения порядка публикации в пределах одного тика лидера.
func (d *Dispatcher) publishBatch(ctx context.Context, events []EventRecord) {
	if len(events) == 0 {
		return
	}

	start := time.Now()
	defer func() {
		d.metrics.RecordDuration("outbox.publish_batch.duration", time.Since(start), nil)
	}()

	processed, failed := 0, 0

	for _, event := range events {
		if err := d.publishEvent(ctx, event); err != nil {
			failed++
			d.metrics.IncrementCounter("outbox.publish_batch.processed", map[string]string{"status": "failed"})
			d.logger.Error("outbox: failed to process event", zap.Int64("id", event.ID), zap.Error(err))
			continue
		}
		processed++
		d.metrics.IncrementCounter("outbox.publish_batch.processed", map[string]string{"status": "success"})
	}

	d.logger.Debug("outbox: batch processing completed",
		zap.Int("processed", processed),
		zap.Int("failed", failed),
	)
}

func (d *Dispatcher) publishEvent(ctx context.Context, event EventRecord) error {
	select {
	case <-ctx.Done():
		return fmt.Errorf("context cancelled: %w", ctx.Err())
	default:
	}

	fields := []zap.Field{
		zap.Int64("id", event.ID),
		zap.String("event_id", event.EventID),
		zap.String("event_type", event.EventType),
		zap.String("aggregate_id", event.AggregateID),
		zap.String("aggregate_type", event.AggregateType),
		zap.String("topic", event.Topic),
		zap.Int("attempt", event.AttemptCount),
	}

	attempt := event.AttemptCount + 1

	if err := d.publisher.Publish(ctx, event); err != nil {
		d.metrics.IncrementCounter("outbox.publish.result", map[string]string{
			"event_type": event.EventType,
			"status":     "failed",
		})
		d.logger.Error("outbox: failed to publish event to kafka", append(fields, zap.Error(err))...)
		return d.handlePublishError(ctx, event, attempt, err)
	}

	d.metrics.IncrementCounter("outbox.publish.result", map[string]string{
		"event_type": event.EventType,
		"status":     "success",
	})
	d.logger.Debug("outbox: event published successfully", fields...)

	return d.handlePublishSuccess(ctx, event, attempt)
}

func (d *Dispatcher) handlePublishError(ctx context.Context, event EventRecord, attempt int, publishErr error) error {
	fields := []zap.Field{
		zap.Int64("id", event.ID),
		zap.String("event_id", event.EventID),
		zap.Int("attempt", attempt),
		zap.Int("max_attempts", d.maxAttempts),
	}

	var newStatus int
	var nextAttemptAt *time.Time

	if attempt >= d.maxAttempts {
		newStatus = EventRecordStatusError
		d.logger.Error("outbox: event exceeded max attempts, marking as error", append(fields, zap.Error(publishErr))...)
		d.metrics.IncrementCounter("outbox.publish.max_attempts_exceeded", map[string]string{"event_type": event.EventType})
	} else {
		newStatus = EventRecordStatusRetry
		nextAttempt := d.backoffStrategy.CalculateNextAttempt(attempt)
		nextAttemptAt = &nextAttempt
		d.logger.Info("outbox: scheduling event for retry", append(fields, zap.Time("next_attempt", nextAttempt), zap.Error(publishErr))...)
		d.metrics.IncrementCounter("outbox.publish.retry_scheduled", map[string]string{"event_type": event.EventType})
	}

	if err := d.updateStatus(ctx, event.ID, newStatus, attempt, nextAttemptAt, publishErr); err != nil {
		d.logger.Error("outbox: failed to update event status after publish error", append(fields, zap.Error(err))...)
	}

	return fmt.Errorf("failed to publish event %d (attempt %d/%d): %w", event.ID, attempt, d.maxAttempts, publishErr)
}

func (d *Dispatcher) handlePublishSuccess(ctx context.Context, event EventRecord, attempt int) error {
	if err := d.updateStatus(ctx, event.ID, EventRecordStatusSent, attempt, nil, nil); err != nil {
		d.logger.Error("outbox: event published but failed to update status to sent",
			zap.Int64("id", event.ID), zap.Error(err))
		d.metrics.IncrementCounter("outbox.db_update_failed", map[string]string{"operation": "success_handling"})
		return fmt.Errorf("event %d published successfully but failed to update status: %w", event.ID, err)
	}
	return nil
}

func (d *Dispatcher) updateStatus(ctx context.Context, eventID int64, status, attemptCount int, nextAttemptAt *time.Time, lastErr error) error {
	query, args := buildStatusUpdateQuery(eventID, status, attemptCount, nextAttemptAt, lastErr)

	result, err := d.db.ExecContext(ctx, query, args...)
	if err != nil {
		d.metrics.IncrementCounter("outbox.db_update_failed", map[string]string{"operation": "update_status", "reason": "exec_error"})
		return fmt.Errorf("failed to update event %d status to %d: %w", eventID, status, err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected for event %d: %w", eventID, err)
	}
	if rowsAffected == 0 {
		d.metrics.IncrementCounter("outbox.db_update_failed", map[string]string{"operation": "update_status", "reason": "no_rows_affected"})
		return fmt.Errorf("no rows affected when updating event %d status to %d", eventID, status)
	}

	return nil
}

func buildStatusUpdateQuery(eventID int64, status, attemptCount int, nextAttemptAt *time.Time, lastErr error) (string, []interface{}) {
	setParts := []string{"status = ?", "attempt_count = ?"}
	args := []interface{}{status, attemptCount}

	if nextAttemptAt != nil {
		setParts = append(setParts, "next_attempt_at = ?")
		args = append(args, *nextAttemptAt)
	} else {
		setParts = append(setParts, "next_attempt_at = NULL")
	}

	if lastErr != nil {
		errMsg := lastErr.Error()
		if len(errMsg) > 1000 {
			errMsg = errMsg[:1000] + "..."
		}
		setParts = append(setParts, "last_error = ?")
		args = append(args, errMsg)
	} else {
		setParts = append(setParts, "last_error = NULL")
	}

	setParts = append(setParts, "updated_at = NOW(6)")
	args = append(args, eventID)

	query := fmt.Sprintf("UPDATE outbox_events SET %s WHERE id = ?", strings.Join(setParts, ", "))
	return query, args
}
