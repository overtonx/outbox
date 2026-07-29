package outbox

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestMoveDeadLetters_MovesErrorEventsAndDeletes(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	d := &Dispatcher{
		db:        db,
		logger:    zap.NewNop(),
		metrics:   NewNoOpMetricsCollector(),
		batchSize: 10,
	}

	rows := sqlmock.NewRows([]string{
		"id", "event_id", "event_type", "aggregate_type", "aggregate_id", "topic",
		"content_type", "payload", "headers", "attempt_count", "last_error", "created_at",
	}).AddRow(5, "evt-5", "order.created", "order", "order-5", "orders",
		"application/json", []byte(`{}`), nil, 3, "boom", time.Now())

	mock_.ExpectBegin()
	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*WHERE status = \\?").
		WithArgs(EventRecordStatusError, 10).
		WillReturnRows(rows)
	mock_.ExpectExec("INSERT INTO outbox_deadletters").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock_.ExpectExec("DELETE FROM outbox_events WHERE id IN").
		WithArgs(int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock_.ExpectCommit()

	err = d.moveDeadLetters(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestMoveDeadLetters_NoCandidatesIsNoop(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	d := &Dispatcher{
		db:        db,
		logger:    zap.NewNop(),
		metrics:   NewNoOpMetricsCollector(),
		batchSize: 10,
	}

	rows := sqlmock.NewRows([]string{
		"id", "event_id", "event_type", "aggregate_type", "aggregate_id", "topic",
		"content_type", "payload", "headers", "attempt_count", "last_error", "created_at",
	})

	mock_.ExpectBegin()
	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*WHERE status = \\?").
		WillReturnRows(rows)
	mock_.ExpectRollback()

	err = d.moveDeadLetters(context.Background())
	assert.NoError(t, err)
	assert.NoError(t, mock_.ExpectationsWereMet())
}
