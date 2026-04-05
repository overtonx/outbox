package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

var deadLetterColumns = []string{
	"id", "event_id", "event_type", "aggregate_type", "aggregate_id",
	"topic", "content_type", "payload", "headers", "attempt_count", "last_error", "created_at",
}

func newDeadLetterService(db interface{}) *DeadLetterServiceImpl {
	return &DeadLetterServiceImpl{
		logger:    zap.NewNop(),
		batchSize: 100,
		metrics:   NewNoOpMetricsCollector(),
	}
}

func TestMoveToDeadLetters_EmptyBatch(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM outbox_events").
		WillReturnRows(sqlmock.NewRows(deadLetterColumns))
	mock.ExpectRollback()

	svc := &DeadLetterServiceImpl{
		db: db, logger: zap.NewNop(), batchSize: 100, metrics: NewNoOpMetricsCollector(),
	}

	err = svc.MoveToDeadLetters(context.Background())

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestMoveToDeadLetters_HappyPath(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	now := time.Now()
	rows := sqlmock.NewRows(deadLetterColumns).
		AddRow(1, "evt-1", "order.created", "order", "ord-1", "orders", "application/json", []byte(`{}`), nil, 3, "err1", now).
		AddRow(2, "evt-2", "order.created", "order", "ord-2", "orders", "application/json", []byte(`{}`), nil, 3, "err2", now)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM outbox_events").WillReturnRows(rows)
	mock.ExpectExec("INSERT INTO outbox_deadletters").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("INSERT INTO outbox_deadletters").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM outbox_events").WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	svc := &DeadLetterServiceImpl{
		db: db, logger: zap.NewNop(), batchSize: 100, metrics: NewNoOpMetricsCollector(),
	}

	err = svc.MoveToDeadLetters(context.Background())

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// TestMoveToDeadLetters_PartialInsertFailure is the critical regression test for
// the data-loss bug: when some INSERTs into deadletters fail, only successfully
// inserted event IDs must be DELETEd from outbox_events.
func TestMoveToDeadLetters_PartialInsertFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	now := time.Now()
	rows := sqlmock.NewRows(deadLetterColumns).
		AddRow(10, "evt-10", "order.created", "order", "ord-10", "orders", "application/json", []byte(`{}`), nil, 3, nil, now).
		AddRow(20, "evt-20", "order.created", "order", "ord-20", "orders", "application/json", []byte(`{}`), nil, 3, nil, now).
		AddRow(30, "evt-30", "order.created", "order", "ord-30", "orders", "application/json", []byte(`{}`), nil, 3, nil, now)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM outbox_events").WillReturnRows(rows)
	// event 10: success
	mock.ExpectExec("INSERT INTO outbox_deadletters").WillReturnResult(sqlmock.NewResult(1, 1))
	// event 20: failure (e.g. duplicate key)
	mock.ExpectExec("INSERT INTO outbox_deadletters").WillReturnError(errors.New("duplicate key"))
	// event 30: success
	mock.ExpectExec("INSERT INTO outbox_deadletters").WillReturnResult(sqlmock.NewResult(1, 1))
	// DELETE must only include IDs 10 and 30 — NOT 20
	mock.ExpectExec("DELETE FROM outbox_events WHERE id IN").
		WithArgs(int64(10), int64(30)).
		WillReturnResult(sqlmock.NewResult(0, 2))
	mock.ExpectCommit()

	svc := &DeadLetterServiceImpl{
		db: db, logger: zap.NewNop(), batchSize: 100, metrics: NewNoOpMetricsCollector(),
	}

	err = svc.MoveToDeadLetters(context.Background())

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet(), "DELETE must use only successfully inserted IDs")
}

func TestMoveToDeadLetters_AllInsertsFail_NothingDeleted(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	now := time.Now()
	rows := sqlmock.NewRows(deadLetterColumns).
		AddRow(1, "evt-1", "order.created", "order", "ord-1", "orders", "application/json", []byte(`{}`), nil, 3, nil, now)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM outbox_events").WillReturnRows(rows)
	mock.ExpectExec("INSERT INTO outbox_deadletters").WillReturnError(errors.New("insert failed"))
	// No DELETE should happen — no expectations set for it
	mock.ExpectCommit()

	svc := &DeadLetterServiceImpl{
		db: db, logger: zap.NewNop(), batchSize: 100, metrics: NewNoOpMetricsCollector(),
	}

	err = svc.MoveToDeadLetters(context.Background())

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet(), "DELETE must not be called when no INSERTs succeeded")
}

func TestMoveToDeadLetters_DeleteFails_RollsBack(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	now := time.Now()
	rows := sqlmock.NewRows(deadLetterColumns).
		AddRow(1, "evt-1", "order.created", "order", "ord-1", "orders", "application/json", []byte(`{}`), nil, 3, nil, now)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT .* FROM outbox_events").WillReturnRows(rows)
	mock.ExpectExec("INSERT INTO outbox_deadletters").WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("DELETE FROM outbox_events").WillReturnError(errors.New("db error"))
	mock.ExpectRollback()

	svc := &DeadLetterServiceImpl{
		db: db, logger: zap.NewNop(), batchSize: 100, metrics: NewNoOpMetricsCollector(),
	}

	err = svc.MoveToDeadLetters(context.Background())

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to delete events from outbox_events")
}
