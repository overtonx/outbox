package outbox

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// publishBatch публикует все ранее заклеймленные события пачки, не
// дожидаясь подтверждения доставки каждого по отдельности — все Produce()
// отправляются сразу (пайплайн), а сам тик завершается только когда придут
// отчёты о доставке для всех событий. enable.idempotence вместе с
// max.in.flight.requests.per.connection=5 (см. DefaultKafkaConfig)
// гарантирует, что брокер подтверждает сообщения одной партиции (одного
// aggregate_id) строго в том порядке, в котором они были отправлены — так
// порядок публикации в пределах aggregate_id сохраняется даже при
// нескольких одновременно летящих запросах.
func (d *Dispatcher) publishBatch(ctx context.Context, events []EventRecord) {
	if len(events) == 0 {
		return
	}

	start := time.Now()
	defer func() {
		d.metrics.RecordDuration("outbox.publish_batch.duration", time.Since(start), nil)
	}()

	var wg sync.WaitGroup
	wg.Add(len(events))

	for _, event := range events {
		event := event
		attempt := event.AttemptCount + 1

		onDelivery := func(deliveryErr error) {
			defer wg.Done()
			d.completePublish(event, attempt, deliveryErr)
		}

		if err := d.publisher.Publish(ctx, event, onDelivery); err != nil {
			wg.Done()
			d.logger.Error("outbox: failed to enqueue event for publishing",
				zap.Int64("id", event.ID), zap.Error(err))
			d.completePublish(event, attempt, err)
		}
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// Отчёты о доставке, которые придут после этого момента, всё равно
		// будут обработаны — completePublish использует собственный,
		// независимый от ctx контекст для обновления статуса в БД, — но
		// текущий тик не будет их дожидаться, чтобы штатная остановка не
		// зависала на медленном брокере.
		d.logger.Warn("outbox: context cancelled while waiting for kafka delivery reports", zap.Error(ctx.Err()))
	}
}

// completePublish вызывается асинхронно, когда приходит отчёт о доставке
// (или немедленно, если публикация не удалось поставить в очередь).
// Использует собственный ограниченный по времени контекст, а не ctx
// reconcile-тика: отчёт может прийти уже после того, как publishBatch
// перестал ждать (например, из-за отмены ctx при остановке), и финальный
// статус в БД всё равно должен быть записан, а не потерян из-за отменённого
// контекста.
func (d *Dispatcher) completePublish(event EventRecord, attempt int, deliveryErr error) {
	fields := []zap.Field{
		zap.Int64("id", event.ID),
		zap.String("event_id", event.EventID),
		zap.String("event_type", event.EventType),
		zap.String("aggregate_id", event.AggregateID),
		zap.String("aggregate_type", event.AggregateType),
		zap.String("topic", event.Topic),
		zap.Int("attempt", attempt),
	}

	updateCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if deliveryErr != nil {
		d.metrics.IncrementCounter("outbox.publish.result", map[string]string{
			"event_type": event.EventType,
			"status":     "failed",
		})
		d.logger.Error("outbox: failed to publish event to kafka", append(fields, zap.Error(deliveryErr))...)
		d.handlePublishError(updateCtx, event, attempt, deliveryErr)
		return
	}

	d.metrics.IncrementCounter("outbox.publish.result", map[string]string{
		"event_type": event.EventType,
		"status":     "success",
	})
	d.logger.Debug("outbox: event published successfully", fields...)
	d.handlePublishSuccess(updateCtx, event, attempt)
}

func (d *Dispatcher) handlePublishError(ctx context.Context, event EventRecord, attempt int, publishErr error) {
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
}

func (d *Dispatcher) handlePublishSuccess(ctx context.Context, event EventRecord, attempt int) {
	if err := d.updateStatus(ctx, event.ID, EventRecordStatusSent, attempt, nil, nil); err != nil {
		d.logger.Error("outbox: event published but failed to update status to sent",
			zap.Int64("id", event.ID), zap.Error(err))
		d.metrics.IncrementCounter("outbox.db_update_failed", map[string]string{"operation": "success_handling"})
	}
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
