package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// fakeOrderedPublisher — детерминированная реализация OrderedPublisher для
// модульных тестов orchestration-логики Dispatcher'а (ordering.go) без
// реального Kafka: Publish вызывает onDelivery синхронно, поэтому тесты не
// зависят от таймингов горутины-обработчика Events().
type fakeOrderedPublisher struct {
	mu       sync.Mutex
	tracker  *checkpointTracker
	onFatal  func(error)
	recreate int
	calls    []int64
	failIDs  map[int64]error
}

func newFakeOrderedPublisher() *fakeOrderedPublisher {
	return &fakeOrderedPublisher{
		tracker: newCheckpointTracker(0),
		failIDs: make(map[int64]error),
	}
}

func (f *fakeOrderedPublisher) Publish(_ context.Context, event EventRecord, onDelivery func(error)) error {
	f.mu.Lock()
	f.calls = append(f.calls, event.ID)
	failErr, shouldFail := f.failIDs[event.ID]
	if shouldFail {
		delete(f.failIDs, event.ID) // сбой ровно один раз — имитирует успех после replay
	}
	f.mu.Unlock()

	if shouldFail {
		onDelivery(failErr)
		return nil
	}

	f.tracker.Ack(event.ID)
	onDelivery(nil)
	return nil
}

func (f *fakeOrderedPublisher) Close() error { return nil }

func (f *fakeOrderedPublisher) Watermark() int64 { return f.tracker.Watermark() }

func (f *fakeOrderedPublisher) SeedWatermark(id int64) { f.tracker.Reset(id) }

func (f *fakeOrderedPublisher) SetFatalHandler(handler func(error)) {
	f.mu.Lock()
	f.onFatal = handler
	f.mu.Unlock()
}

func (f *fakeOrderedPublisher) Recreate() error {
	f.mu.Lock()
	f.recreate++
	f.mu.Unlock()
	return nil
}

func (f *fakeOrderedPublisher) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls)
}

func eventRows(ids ...int64) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{
		"id", "event_id", "event_type", "aggregate_type", "aggregate_id", "topic",
		"content_type", "payload", "headers", "attempt_count", "next_attempt_at",
	})
	for _, id := range ids {
		rows.AddRow(id, "evt", "t", "agg", "a1", "orders", "application/json", []byte(`{}`), nil, 0, nil)
	}
	return rows
}

func TestClaimAndPublishOrdered_PublishesInOrderAndAdvancesHighestClaimed(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := newFakeOrderedPublisher()
	d := &Dispatcher{
		db:             db,
		logger:         zap.NewNop(),
		metrics:        NewNoOpMetricsCollector(),
		ordered:        publisher,
		batchSize:      10,
		inFlightWindow: 10,
	}

	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*WHERE id > \\?").
		WithArgs(int64(0), 10).
		WillReturnRows(eventRows(1, 2, 3))

	highest, err := d.claimAndPublishOrdered(context.Background(), 0, func(error) {})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), highest)
	assert.Equal(t, []int64{1, 2, 3}, publisher.calls)
	assert.Equal(t, int64(3), publisher.Watermark())
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestClaimAndPublishOrdered_BoundsClaimToInFlightWindow(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := newFakeOrderedPublisher()
	publisher.tracker.Reset(0) // watermark=0, ничего ещё не подтверждено
	d := &Dispatcher{
		db:             db,
		logger:         zap.NewNop(),
		metrics:        NewNoOpMetricsCollector(),
		ordered:        publisher,
		batchSize:      100,
		inFlightWindow: 3, // окно уже "занято" highestClaimed-watermark=3
	}

	// highestClaimed=3, watermark=0 -> окно уже полностью занято, claim не должен идти в БД.
	highest, err := d.claimAndPublishOrdered(context.Background(), 3, func(error) {})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), highest)
	assert.Empty(t, publisher.calls)
	assert.NoError(t, mock_.ExpectationsWereMet(), "no query should have been issued when the window is full")
}

func TestClaimAndPublishOrdered_DeliveryFailureReportsFaultButDoesNotStopBatch(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := newFakeOrderedPublisher()
	publisher.failIDs[2] = errors.New("delivery failed")
	d := &Dispatcher{
		db:             db,
		logger:         zap.NewNop(),
		metrics:        NewNoOpMetricsCollector(),
		ordered:        publisher,
		batchSize:      10,
		inFlightWindow: 10,
	}

	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*WHERE id > \\?").
		WithArgs(int64(0), 10).
		WillReturnRows(eventRows(1, 2, 3))

	var faults []error
	highest, err := d.claimAndPublishOrdered(context.Background(), 0, func(e error) {
		faults = append(faults, e)
	})
	assert.NoError(t, err)
	// Пайплайн продолжает публиковать весь заклеймленный батч — fire-and-forget,
	// остановку и replay делает вызывающий цикл (runOrderedLeader), а не сам claim.
	assert.Equal(t, []int64{1, 2, 3}, publisher.calls)
	assert.Equal(t, int64(3), highest)
	assert.Len(t, faults, 1)
	// watermark застревает на 1: 2 не подтверждён (сбой), 3 подтверждён, но
	// не может продвинуть watermark из-за дыры на 2 — ack по непрерывному
	// префиксу, не по последнему пришедшему.
	assert.Equal(t, int64(1), publisher.Watermark())
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestFlushCheckpoint_PersistsWatermarkAndBatchUpdatesStatus(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := newFakeOrderedPublisher()
	publisher.tracker.Reset(7)
	d := &Dispatcher{
		db:          db,
		logger:      zap.NewNop(),
		metrics:     NewNoOpMetricsCollector(),
		ordered:     publisher,
		checkpoints: newCheckpointStore(db),
		lockName:    "lock-1",
	}

	mock_.ExpectExec("INSERT INTO outbox_checkpoints .* ON DUPLICATE KEY UPDATE").
		WithArgs("lock-1", int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock_.ExpectExec("UPDATE outbox_events SET status = \\? WHERE id <= \\? AND status <> \\?").
		WithArgs(EventRecordStatusSent, int64(7), EventRecordStatusSent).
		WillReturnResult(sqlmock.NewResult(0, 7))

	d.flushCheckpoint(context.Background())
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestQuarantineAndAdvance_MovesEventAndForceAdvancesCheckpoint(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	d := &Dispatcher{
		db:          db,
		logger:      zap.NewNop(),
		metrics:     NewNoOpMetricsCollector(),
		checkpoints: newCheckpointStore(db),
		lockName:    "lock-1",
	}

	mock_.ExpectBegin()
	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*WHERE id = \\?(.|\n)*FOR UPDATE").
		WithArgs(int64(3)).
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "event_id", "event_type", "aggregate_type", "aggregate_id", "topic",
			"content_type", "payload", "headers", "attempt_count", "created_at",
		}).AddRow(3, "evt-3", "t", "agg", "a1", "orders", "application/json", []byte(`{}`), nil, 0, time.Now()))
	mock_.ExpectExec("INSERT INTO outbox_deadletters").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock_.ExpectExec("DELETE FROM outbox_events WHERE id = \\?").
		WithArgs(int64(3)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock_.ExpectCommit()

	mock_.ExpectExec("INSERT INTO outbox_checkpoints .* ON DUPLICATE KEY UPDATE").
		WithArgs("lock-1", int64(3)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = d.quarantineAndAdvance(context.Background(), 3, errors.New("poison"))
	assert.NoError(t, err)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestPoisonStreakTracker_TracksConsecutiveFailuresOnSameID(t *testing.T) {
	tracker := &poisonStreakTracker{atID: -1}

	assert.Equal(t, 1, tracker.Record(5))
	assert.Equal(t, 2, tracker.Record(5))
	assert.Equal(t, 3, tracker.Record(5))

	// сбой на другом id сбрасывает серию — это не тот же самый poison-message
	assert.Equal(t, 1, tracker.Record(9))

	tracker.Reset()
	assert.Equal(t, 1, tracker.Record(9))
}

func TestFakeOrderedPublisher_ImplementsOrderedPublisher(t *testing.T) {
	var _ OrderedPublisher = newFakeOrderedPublisher()
}

// TestRunOrderedLeader_FaultTriggersRecreateAndReplayFromPersistedCheckpoint
// прогоняет полный orchestration-цикл (ordering.go): claim публикует 1..5,
// доставка id=3 падает -> цикл обязан среагировать остановкой всего потока и
// replay с чекпоинта (а не точечным ретраем одного id=3), пересоздать
// producer и переопубликовать 1..5 заново с нуля (чекпоинт ещё ни разу не
// флашился), в этот раз успешно. pollInterval/checkpointFlushInterval
// выставлены в час, чтобы тикеры не мешали детерминированности теста —
// единственное событие, продвигающее цикл, это канал fault.
func TestRunOrderedLeader_FaultTriggersRecreateAndReplayFromPersistedCheckpoint(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	publisher := newFakeOrderedPublisher()
	publisher.failIDs[3] = errors.New("boom")

	d := &Dispatcher{
		db:                      db,
		logger:                  zap.NewNop(),
		metrics:                 NewNoOpMetricsCollector(),
		ordered:                 publisher,
		checkpoints:             newCheckpointStore(db),
		lockName:                "lock-1",
		batchSize:               10,
		inFlightWindow:          10,
		pollInterval:            time.Hour,
		checkpointFlushInterval: time.Hour,
		poisonMessageThreshold:  5,
	}

	mock_.ExpectQuery("SELECT last_confirmed_id FROM outbox_checkpoints WHERE lock_name = \\?").
		WithArgs("lock-1").
		WillReturnRows(sqlmock.NewRows([]string{"last_confirmed_id"}))

	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*WHERE id > \\?").
		WithArgs(int64(0), 10).
		WillReturnRows(eventRows(1, 2, 3, 4, 5))

	mock_.ExpectQuery("SELECT last_confirmed_id FROM outbox_checkpoints WHERE lock_name = \\?").
		WithArgs("lock-1").
		WillReturnRows(sqlmock.NewRows([]string{"last_confirmed_id"}))

	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*WHERE id > \\?").
		WithArgs(int64(0), 10).
		WillReturnRows(eventRows(1, 2, 3, 4, 5))

	mock_.ExpectExec("INSERT INTO outbox_checkpoints .* ON DUPLICATE KEY UPDATE").
		WithArgs("lock-1", int64(5)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock_.ExpectExec("UPDATE outbox_events SET status = \\? WHERE id <= \\? AND status <> \\?").
		WithArgs(EventRecordStatusSent, int64(5), EventRecordStatusSent).
		WillReturnResult(sqlmock.NewResult(0, 5))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.runOrderedLeader(ctx)
		close(done)
	}()

	assert.Eventually(t, func() bool {
		return publisher.Watermark() == 5
	}, 2*time.Second, 5*time.Millisecond, "watermark should reach 5 after fault+replay")

	publisher.mu.Lock()
	recreateCount := publisher.recreate
	calls := append([]int64(nil), publisher.calls...)
	publisher.mu.Unlock()

	assert.Equal(t, 1, recreateCount, "exactly one Recreate for the single fault")
	assert.Equal(t, []int64{1, 2, 3, 4, 5, 1, 2, 3, 4, 5}, calls,
		"replay must re-publish the whole unconfirmed range in order, not just retry id=3 in place")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runOrderedLeader did not exit after ctx cancellation")
	}

	assert.NoError(t, mock_.ExpectationsWereMet())
}
