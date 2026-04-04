package outbox

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func newTestProcessor(db *sql.DB) *EventProcessorImpl {
	return &EventProcessorImpl{
		db:              db,
		logger:          zap.NewNop(),
		backoffStrategy: DefaultBackoffStrategy(),
		maxAttempts:     3,
		batchSize:       10,
		publisher:       &MockPublisher{},
		metrics:         NewNoOpMetricsCollector(),
	}
}

// --- buildUpdateQuery ---

func TestBuildUpdateQuery_AllFields(t *testing.T) {
	p := &EventProcessorImpl{}
	nextAttempt := time.Now().Add(time.Minute)
	lastErr := errors.New("kafka timeout")

	query, args, err := p.buildUpdateQuery(42, EventRecordStatusRetry, 2, &nextAttempt, lastErr)

	assert.NoError(t, err)
	assert.Contains(t, query, "status = ?")
	assert.Contains(t, query, "attempt_count = ?")
	assert.Contains(t, query, "next_attempt_at = ?")
	assert.Contains(t, query, "last_error = ?")
	assert.Contains(t, query, "updated_at = NOW()")
	assert.Contains(t, query, "WHERE id = ?")
	assert.Equal(t, EventRecordStatusRetry, args[0])
	assert.Equal(t, 2, args[1])
	assert.Equal(t, nextAttempt, args[2])
	assert.Equal(t, "kafka timeout", args[3])
	assert.Equal(t, int64(42), args[4])
}

func TestBuildUpdateQuery_NilFields(t *testing.T) {
	p := &EventProcessorImpl{}

	query, args, err := p.buildUpdateQuery(1, EventRecordStatusSent, 1, nil, nil)

	assert.NoError(t, err)
	assert.Contains(t, query, "next_attempt_at = NULL")
	assert.Contains(t, query, "last_error = NULL")
	assert.Equal(t, EventRecordStatusSent, args[0])
	assert.Equal(t, 1, args[1])
	assert.Equal(t, int64(1), args[2])
}

func TestBuildUpdateQuery_ErrorTruncatedAt1000Chars(t *testing.T) {
	p := &EventProcessorImpl{}
	lastErr := errors.New(strings.Repeat("x", 1500))

	_, args, err := p.buildUpdateQuery(1, EventRecordStatusError, 3, nil, lastErr)

	assert.NoError(t, err)
	errArg, ok := args[2].(string)
	assert.True(t, ok)
	assert.Equal(t, 1003, len(errArg)) // 1000 + "..."
	assert.True(t, strings.HasSuffix(errArg, "..."))
}

// --- handlePublishSuccess ---

func TestHandlePublishSuccess(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("UPDATE outbox_events SET").
		WillReturnResult(sqlmock.NewResult(0, 1))

	p := newTestProcessor(db)
	event := EventRecord{ID: 10, EventType: "test", AggregateID: "agg-1"}

	err = p.handlePublishSuccess(context.Background(), event, 1)

	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandlePublishSuccess_DBError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("UPDATE outbox_events SET").
		WillReturnError(errors.New("db connection lost"))

	p := newTestProcessor(db)
	event := EventRecord{ID: 10, EventType: "test", AggregateID: "agg-1"}

	err = p.handlePublishSuccess(context.Background(), event, 1)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "published successfully but failed to update status")
}

func TestHandlePublishSuccess_NoRowsAffected(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("UPDATE outbox_events SET").
		WillReturnResult(sqlmock.NewResult(0, 0))

	p := newTestProcessor(db)
	event := EventRecord{ID: 10, EventType: "test", AggregateID: "agg-1"}

	err = p.handlePublishSuccess(context.Background(), event, 1)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "no rows affected")
}

// --- handlePublishError ---

func TestHandlePublishError_SchedulesRetry(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("UPDATE outbox_events SET").
		WillReturnResult(sqlmock.NewResult(0, 1))

	p := newTestProcessor(db)
	p.maxAttempts = 3
	event := EventRecord{ID: 5, EventType: "test", AggregateID: "agg-1"}
	publishErr := errors.New("broker unavailable")

	err = p.handlePublishError(context.Background(), event, 1, publishErr) // attempt 1 < max 3

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "broker unavailable")
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandlePublishError_MaxAttemptsExceeded_SetsErrorStatus(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("UPDATE outbox_events SET").
		WillReturnResult(sqlmock.NewResult(0, 1))

	p := newTestProcessor(db)
	p.maxAttempts = 3
	event := EventRecord{ID: 5, EventType: "test", AggregateID: "agg-1"}

	err = p.handlePublishError(context.Background(), event, 3, errors.New("kafka down")) // attempt 3 == max

	assert.Error(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestHandlePublishError_DBUpdateFails_StillReturnsPublishError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock.ExpectExec("UPDATE outbox_events SET").
		WillReturnError(errors.New("db down"))

	p := newTestProcessor(db)
	p.maxAttempts = 3
	event := EventRecord{ID: 5, EventType: "test", AggregateID: "agg-1"}
	publishErr := errors.New("original kafka error")

	err = p.handlePublishError(context.Background(), event, 1, publishErr)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "original kafka error")
}

// --- updateStatus ---

func TestUpdateStatus_InvalidEventID(t *testing.T) {
	p := newTestProcessor(nil)
	err := p.updateStatus(context.Background(), 0, EventRecordStatusSent, 1, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid event ID")
}

func TestUpdateStatus_InvalidStatus(t *testing.T) {
	p := newTestProcessor(nil)
	assert.Error(t, p.updateStatus(context.Background(), 1, -1, 1, nil, nil))
	assert.Error(t, p.updateStatus(context.Background(), 1, 5, 1, nil, nil))
}

func TestUpdateStatus_InvalidAttemptCount(t *testing.T) {
	p := newTestProcessor(nil)
	err := p.updateStatus(context.Background(), 1, EventRecordStatusSent, -1, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "invalid attempt count")
}
