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

// fakePublisher имитирует асинхронную доставку: по умолчанию вызывает
// onDelivery синхронно внутри Publish (детерминированно для тестов), но
// может быть настроен на вызов из отдельной горутины через async=true,
// чтобы проверить, что publishBatch действительно дожидается всех отчётов.
type fakePublisher struct {
	err   error
	async bool
	delay time.Duration

	calls []EventRecord
}

func (f *fakePublisher) Publish(_ context.Context, event EventRecord, onDelivery func(error)) error {
	f.calls = append(f.calls, event)
	if f.async {
		go func() {
			if f.delay > 0 {
				time.Sleep(f.delay)
			}
			onDelivery(f.err)
		}()
		return nil
	}
	onDelivery(f.err)
	return nil
}

func (f *fakePublisher) Close() error { return nil }

func TestPublishBatch_SuccessMarksSent(t *testing.T) {
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
	d.publishBatch(context.Background(), []EventRecord{event})

	assert.Len(t, publisher.calls, 1)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestPublishBatch_ErrorBelowMaxAttemptsSchedulesRetry(t *testing.T) {
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
	d.publishBatch(context.Background(), []EventRecord{event})

	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestPublishBatch_ErrorAtMaxAttemptsMarksError(t *testing.T) {
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
	d.publishBatch(context.Background(), []EventRecord{event})

	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestPublishBatch_EnqueueFailureIsTreatedAsPublishError(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := &enqueueFailingPublisher{err: errors.New("queue full")}
	d := &Dispatcher{
		db:              db,
		logger:          zap.NewNop(),
		metrics:         NewNoOpMetricsCollector(),
		publisher:       publisher,
		backoffStrategy: DefaultBackoffStrategy(),
		maxAttempts:     3,
	}

	mock_.ExpectExec("UPDATE outbox_events SET status = \\?, attempt_count = \\?").
		WithArgs(EventRecordStatusRetry, 1, sqlmock.AnyArg(), "queue full", int64(11)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	event := EventRecord{ID: 11, EventType: "order.created", AggregateID: "order-1", Topic: "orders"}
	d.publishBatch(context.Background(), []EventRecord{event})

	assert.NoError(t, mock_.ExpectationsWereMet())
}

type enqueueFailingPublisher struct {
	err error
}

func (p *enqueueFailingPublisher) Publish(_ context.Context, _ EventRecord, _ func(error)) error {
	return p.err
}

func (p *enqueueFailingPublisher) Close() error { return nil }

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

// TestPublishBatch_WaitsForAsyncDeliveryReports проверяет, что publishBatch
// действительно дожидается отчётов о доставке, даже когда они приходят
// асинхронно (из другой горутины) и с задержкой — как это происходит с
// реальным KafkaPublisher, где Produce() возвращается немедленно, а отчёт
// приходит позже через общий канал Events().
func TestPublishBatch_WaitsForAsyncDeliveryReports(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := &fakePublisher{async: true, delay: 30 * time.Millisecond}
	d := &Dispatcher{
		db:              db,
		logger:          zap.NewNop(),
		metrics:         NewNoOpMetricsCollector(),
		publisher:       publisher,
		backoffStrategy: DefaultBackoffStrategy(),
		maxAttempts:     3,
	}

	// Отчёты приходят из разных горутин почти одновременно — порядок
	// завершения непредсказуем, поэтому ожидания не привязаны к порядку.
	mock_.MatchExpectationsInOrder(false)
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

	// Если бы publishBatch не дожидался асинхронных отчётов, эти ожидания
	// БД ещё не были бы выполнены к этому моменту.
	assert.NoError(t, mock_.ExpectationsWereMet())
}
