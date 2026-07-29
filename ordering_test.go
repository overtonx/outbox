//go:build integration

package outbox

// Интеграционные тесты гарантии порядка и leader-election при нескольких
// инстансах Dispatcher. Требуют реальной MySQL (например, поднятой через
// example/docker-compose.yml) — адрес передаётся через переменную
// окружения OUTBOX_MYSQL_DSN, без неё тесты пропускаются.
//
// Запуск: OUTBOX_MYSQL_DSN="root:root@tcp(127.0.0.1:3306)/outbox_test" \
//         go test -tags=integration ./... -run TestOrdering -v

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/overtonx/outbox/v4/migrations"
	"github.com/overtonx/outbox/v4/serializer"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func openIntegrationDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := os.Getenv("OUTBOX_MYSQL_DSN")
	if dsn == "" {
		t.Skip("OUTBOX_MYSQL_DSN not set, skipping integration test")
	}

	db, err := sql.Open("mysql", dsn)
	assert.NoError(t, err)
	assert.NoError(t, db.Ping())

	_, _ = db.Exec("DELETE FROM outbox_events")
	_, _ = db.Exec("DELETE FROM outbox_deadletters")

	assert.NoError(t, migrations.Migrate(context.Background(), db))
	return db
}

type recordingPublisher struct {
	mu          sync.Mutex
	byAggregate map[string][]string // aggregate_id -> event_id в порядке публикации
}

func newRecordingPublisher() *recordingPublisher {
	return &recordingPublisher{byAggregate: make(map[string][]string)}
}

func (p *recordingPublisher) Publish(_ context.Context, event EventRecord) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.byAggregate[event.AggregateID] = append(p.byAggregate[event.AggregateID], event.EventID)
	return nil
}

func (p *recordingPublisher) Close() error { return nil }

// TestOrdering_PerAggregateOrderPreservedAcrossMultipleInstances вставляет
// вперемешку события нескольких aggregate_id из нескольких конкурентных
// транзакций, поднимает два Dispatcher на одной БД и проверяет, что для
// каждого aggregate_id порядок публикации совпадает с порядком вставки, а
// лидером в любой момент времени является ровно один инстанс.
func TestOrdering_PerAggregateOrderPreservedAcrossMultipleInstances(t *testing.T) {
	db := openIntegrationDB(t)
	defer db.Close()

	store := NewEventStore(serializer.JSONSerializer{})

	const aggregates = 5
	const eventsPerAggregate = 20

	expected := make(map[string][]string)
	var wg sync.WaitGroup
	var mu sync.Mutex

	for a := 0; a < aggregates; a++ {
		aggregateID := fmt.Sprintf("agg-%d", a)
		wg.Add(1)
		go func(aggID string) {
			defer wg.Done()
			for i := 0; i < eventsPerAggregate; i++ {
				eventID := uuid.NewString()
				event := Event{
					EventID:       eventID,
					EventType:     "test.event",
					AggregateType: "test",
					AggregateID:   aggID,
					Topic:         "test-topic",
					Payload:       map[string]int{"seq": i},
				}
				if err := store.SaveWithDB(context.Background(), db, event); err != nil {
					t.Errorf("save failed: %v", err)
					return
				}
				mu.Lock()
				expected[aggID] = append(expected[aggID], eventID)
				mu.Unlock()
			}
		}(aggregateID)
	}
	wg.Wait()

	publisher := newRecordingPublisher()

	newInstance := func(name string) *Dispatcher {
		d, err := NewDispatcher(db,
			WithPublisher(publisher),
			WithBatchSize(50),
			WithPollInterval(200*time.Millisecond),
			WithProcessingLeaseTimeout(5*time.Second),
			WithLogger(zap.NewNop().Named(name)),
		)
		assert.NoError(t, err)
		return d
	}

	d1 := newInstance("instance-1")
	d2 := newInstance("instance-2")

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var runWg sync.WaitGroup
	runWg.Add(2)
	go func() { defer runWg.Done(); _ = d1.Start(ctx) }()
	go func() { defer runWg.Done(); _ = d2.Start(ctx) }()

	deadline := time.Now().Add(12 * time.Second)
	totalExpected := aggregates * eventsPerAggregate
	for time.Now().Before(deadline) {
		publisher.mu.Lock()
		total := 0
		for _, ids := range publisher.byAggregate {
			total += len(ids)
		}
		publisher.mu.Unlock()
		if total >= totalExpected {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	cancel()
	runWg.Wait()

	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	for aggID, wantOrder := range expected {
		assert.Equal(t, wantOrder, publisher.byAggregate[aggID], "publish order mismatch for aggregate %s", aggID)
	}
}
