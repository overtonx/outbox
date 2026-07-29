package outbox

import (
	"context"
	"database/sql"
	"fmt"

	"go.uber.org/zap"
)

// claimBatch — единственный запрос, отвечающий и за клейминг новых/готовых
// к ретраю событий, и за восстановление после падения: строки, зависшие в
// processing дольше processingLeaseTimeout (например, инстанс упал между
// коммитом claim-транзакции и обновлением финального статуса), попадают в
// тот же результат, что и обычные новые/retry строки, без отдельного
// тикера/сервиса. Выполняется только текущим лидером (см. leader.go), что и
// даёт полный глобальный порядок публикации при нескольких инстансах.
func (d *Dispatcher) claimBatch(ctx context.Context) ([]EventRecord, error) {
	tx, err := d.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("failed to begin claim transaction: %w", err)
	}
	defer tx.Rollback()

	leaseSeconds := int(d.processingLeaseTimeout.Seconds())

	query := `
		SELECT id, event_id, event_type, aggregate_type, aggregate_id, topic,
		       content_type, payload, headers, attempt_count, next_attempt_at
		FROM outbox_events
		WHERE status = ?
		   OR (status = ? AND next_attempt_at <= NOW(6))
		   OR (status = ? AND updated_at <= NOW(6) - INTERVAL ? SECOND)
		ORDER BY created_at ASC, id ASC
		LIMIT ?
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.QueryContext(ctx, query,
		EventRecordStatusNew,
		EventRecordStatusRetry,
		EventRecordStatusProcessing, leaseSeconds,
		d.batchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query claimable events: %w", err)
	}

	var events []EventRecord
	var eventIDs []int64

	for rows.Next() {
		var event EventRecord
		var nextAttemptAt sql.NullTime

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
			&nextAttemptAt,
		); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan claimed event: %w", err)
		}

		if nextAttemptAt.Valid {
			event.NextAttemptAt = &nextAttemptAt.Time
		}

		events = append(events, event)
		eventIDs = append(eventIDs, event.ID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, fmt.Errorf("failed to iterate claimed events: %w", err)
	}
	rows.Close()

	if len(eventIDs) > 0 {
		updateQuery := fmt.Sprintf(
			"UPDATE outbox_events SET status = ?, updated_at = NOW(6) WHERE id IN (%s)",
			placeholders(len(eventIDs)),
		)

		args := make([]interface{}, 0, len(eventIDs)+1)
		args = append(args, EventRecordStatusProcessing)
		for _, id := range eventIDs {
			args = append(args, id)
		}

		if _, err := tx.ExecContext(ctx, updateQuery, args...); err != nil {
			return nil, fmt.Errorf("failed to mark events as processing: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("failed to commit claim transaction: %w", err)
	}

	if len(events) > 0 {
		d.logger.Debug("outbox: claimed events for processing", zap.Int("count", len(events)))
	}

	return events, nil
}
