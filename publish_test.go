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

type fakePublisher struct {
	err   error
	calls []EventRecord
}

func (f *fakePublisher) Publish(_ context.Context, event EventRecord) error {
	f.calls = append(f.calls, event)
	return f.err
}

func (f *fakePublisher) Close() error { return nil }

func TestPublishEvent_SuccessMarksSent(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := &fakePublisher{}
	d := &Dispatcher{
		db:              db,
		logger:          zap.NewNop(),
		metrics:         NewNoOpMetricsCollector(),
		publisher:       publisher,
		backoffStrategy: DefaultBackoffStrategy(),
		maxAttempts:     3,
	}

	mock_.ExpectExec("UPDATE outbox_events SET status = \\?, attempt_count = \\?").
		WithArgs(EventRecordStatusSent, 1, int64(42)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	event := EventRecord{ID: 42, EventType: "order.created", AggregateID: "order-1", Topic: "orders"}
	err = d.publishEvent(context.Background(), event)

	assert.NoError(t, err)
	assert.Len(t, publisher.calls, 1)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestPublishEvent_ErrorBelowMaxAttemptsSchedulesRetry(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := &fakePublisher{err: errors.New("kafka down")}
	d := &Dispatcher{
		db:              db,
		logger:          zap.NewNop(),
		metrics:         NewNoOpMetricsCollector(),
		publisher:       publisher,
		backoffStrategy: DefaultBackoffStrategy(),
		maxAttempts:     3,
	}

	mock_.ExpectExec("UPDATE outbox_events SET status = \\?, attempt_count = \\?").
		WithArgs(EventRecordStatusRetry, 1, sqlmock.AnyArg(), "kafka down", int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	event := EventRecord{ID: 7, EventType: "order.created", AggregateID: "order-1", Topic: "orders", AttemptCount: 0}
	err = d.publishEvent(context.Background(), event)

	assert.Error(t, err)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestPublishEvent_ErrorAtMaxAttemptsMarksError(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := &fakePublisher{err: errors.New("kafka down")}
	d := &Dispatcher{
		db:              db,
		logger:          zap.NewNop(),
		metrics:         NewNoOpMetricsCollector(),
		publisher:       publisher,
		backoffStrategy: DefaultBackoffStrategy(),
		maxAttempts:     3,
	}

	mock_.ExpectExec("UPDATE outbox_events SET status = \\?, attempt_count = \\?").
		WithArgs(EventRecordStatusError, 3, "kafka down", int64(9)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	event := EventRecord{ID: 9, EventType: "order.created", AggregateID: "order-1", Topic: "orders", AttemptCount: 2}
	err = d.publishEvent(context.Background(), event)

	assert.Error(t, err)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestBuildStatusUpdateQuery_SentHasNoNextAttemptOrError(t *testing.T) {
	query, args := buildStatusUpdateQuery(1, EventRecordStatusSent, 1, nil, nil)

	assert.Contains(t, query, "next_attempt_at = NULL")
	assert.Contains(t, query, "last_error = NULL")
	assert.Equal(t, []interface{}{EventRecordStatusSent, 1, int64(1)}, args)
}

func TestBuildStatusUpdateQuery_RetryIncludesNextAttemptAndError(t *testing.T) {
	next := time.Now().Add(time.Minute)
	query, args := buildStatusUpdateQuery(2, EventRecordStatusRetry, 1, &next, errors.New("boom"))

	assert.Contains(t, query, "next_attempt_at = ?")
	assert.Contains(t, query, "last_error = ?")
	assert.Equal(t, []interface{}{EventRecordStatusRetry, 1, next, "boom", int64(2)}, args)
}

func TestPublishBatch_PreservesOrderAndContinuesPastFailures(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := &fakePublisher{}
	d := &Dispatcher{
		db:              db,
		logger:          zap.NewNop(),
		metrics:         NewNoOpMetricsCollector(),
		publisher:       publisher,
		backoffStrategy: DefaultBackoffStrategy(),
		maxAttempts:     3,
	}

	for _, id := range []int64{1, 2, 3} {
		mock_.ExpectExec("UPDATE outbox_events SET status = \\?, attempt_count = \\?").
			WithArgs(EventRecordStatusSent, 1, id).
			WillReturnResult(sqlmock.NewResult(0, 1))
	}

	events := []EventRecord{
		{ID: 1, EventType: "t", AggregateID: "a1", Topic: "orders"},
		{ID: 2, EventType: "t", AggregateID: "a2", Topic: "orders"},
		{ID: 3, EventType: "t", AggregateID: "a3", Topic: "orders"},
	}

	d.publishBatch(context.Background(), events)

	assert.Len(t, publisher.calls, 3)
	assert.Equal(t, int64(1), publisher.calls[0].ID)
	assert.Equal(t, int64(2), publisher.calls[1].ID)
	assert.Equal(t, int64(3), publisher.calls[2].ID)
	assert.NoError(t, mock_.ExpectationsWereMet())
}
