package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// moveDeadLetters переносит события, исчерпавшие максимум попыток
// публикации, из outbox_events в outbox_deadletters. Выполняется тем же
// лидером сразу после claim+publish, в рамках того же тика — отдельного
// воркера/интервала для этого не требуется.
func (d *Dispatcher) moveDeadLetters(ctx context.Context) error {
	start := time.Now()
	defer func() {
		d.metrics.RecordDuration("outbox.deadletter.move.duration", time.Since(start), nil)
	}()

	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin deadletter transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, topic,
		       content_type, payload, headers, attempt_count, last_error, created_at
		FROM outbox_events
		WHERE status = ?
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query, EventRecordStatusError, d.batchSize)
	if err != nil {
		return fmt.Errorf("failed to query error events: %w", err)
	}

	var candidates []DeadLetterRecord
	for rows.Next() {
		var event DeadLetterRecord
		var lastError sql.NullString

		if err := rows.Scan(
			&event.ID,
			&event.EventID,
			&event.EventType,
			&event.AggregateType,
			&event.AggregateID,
			&event.Topic,
			&event.ContentType,
			&event.Payload,
			&event.Headers,
			&event.AttemptCount,
			&lastError,
			&event.CreatedAt,
		); err != nil {
			rows.Close()
			return fmt.Errorf("failed to scan error event: %w", err)
		}

		if lastError.Valid {
			event.LastError = lastError.String
		}
		candidates = append(candidates, event)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("failed to iterate error events: %w", err)
	}
	rows.Close()

	if len(candidates) == 0 {
		return nil
	}

	insertQuery := `
		INSERT INTO outbox_deadletters
		(id, event_id, event_type, aggregate_type, aggregate_id, topic,
		 content_type, payload, headers, attempt_count, last_error, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	movedIDs := make([]int64, 0, len(candidates))
	for _, event := range candidates {
		if _, err := tx.ExecContext(ctx, insertQuery,
			event.ID,
			event.EventID,
			event.EventType,
			event.AggregateType,
			event.AggregateID,
			event.Topic,
			event.ContentType,
			event.Payload,
			event.Headers,
			event.AttemptCount,
			nullString(event.LastError),
			event.CreatedAt,
		); err != nil {
			d.logger.Error("outbox: failed to insert into deadletters",
				zap.String("event_id", event.EventID), zap.Error(err))
			continue
		}
		movedIDs = append(movedIDs, event.ID)
	}

	if len(movedIDs) > 0 {
		deleteQuery := fmt.Sprintf("DELETE FROM outbox_events WHERE id IN (%s)", placeholders(len(movedIDs)))
		args := make([]interface{}, len(movedIDs))
		for i, id := range movedIDs {
			args[i] = id
		}
		if _, err := tx.ExecContext(ctx, deleteQuery, args...); err != nil {
			return fmt.Errorf("failed to delete events moved to deadletters: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit deadletter transaction: %w", err)
	}

	d.logger.Info("outbox: moved events to deadletters",
		zap.Int("count", len(movedIDs)),
		zap.Int("total_found", len(candidates)),
	)
	d.metrics.RecordGauge("outbox.deadletter.moved", float64(len(movedIDs)), nil)

	return nil
}

// moveSingleToDeadLetter переносит ровно одно событие (по id) в
// outbox_deadletters с явно переданной причиной. Используется только
// ordered-путём Dispatcher'а (см. ordering.go) как крайняя мера — когда одно
// и то же событие подряд ломает поток через несколько циклов stop/replay
// (poison message) и дальнейшее ожидание блокировало бы публикацию всех
// последующих событий с сохранением порядка навсегда. В отличие от
// moveDeadLetters (построчный retry/backoff путь), здесь нет отдельного
// статуса error — событие уходит в deadletters напрямую из состояния
// "следующее после чекпоинта".
func (d *Dispatcher) moveSingleToDeadLetter(ctx context.Context, id int64, cause error) error {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return fmt.Errorf("failed to begin quarantine transaction: %w", err)
	}
	defer tx.Rollback()

	query := `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, topic,
		       content_type, payload, headers, attempt_count, created_at
		FROM outbox_events
		WHERE id = ?
		FOR UPDATE
	`

	var event DeadLetterRecord
	if err := tx.QueryRowContext(ctx, query, id).Scan(
		&event.ID,
		&event.EventID,
		&event.EventType,
		&event.AggregateType,
		&event.AggregateID,
		&event.Topic,
		&event.ContentType,
		&event.Payload,
		&event.Headers,
		&event.AttemptCount,
		&event.CreatedAt,
	); err != nil {
		if err == sql.ErrNoRows {
			// Событие уже отсутствует (например, унаследовано от прошлого
			// запуска и было убрано вручную) — чекпоинт всё равно можно
			// безопасно продвинуть мимо него.
			return tx.Commit()
		}
		return fmt.Errorf("failed to load quarantined event %d: %w", id, err)
	}
	event.LastError = cause.Error()

	insertQuery := `
		INSERT INTO outbox_deadletters
		(id, event_id, event_type, aggregate_type, aggregate_id, topic,
		 content_type, payload, headers, attempt_count, last_error, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	if _, err := tx.ExecContext(ctx, insertQuery,
		event.ID,
		event.EventID,
		event.EventType,
		event.AggregateType,
		event.AggregateID,
		event.Topic,
		event.ContentType,
		event.Payload,
		event.Headers,
		event.AttemptCount,
		nullString(event.LastError),
		event.CreatedAt,
	); err != nil {
		return fmt.Errorf("failed to insert quarantined event %d into deadletters: %w", id, err)
	}

	if _, err := tx.ExecContext(ctx, "DELETE FROM outbox_events WHERE id = ?", id); err != nil {
		return fmt.Errorf("failed to delete quarantined event %d: %w", id, err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit quarantine transaction: %w", err)
	}

	d.metrics.IncrementCounter("outbox.deadletter.quarantined", nil)
	return nil
}
