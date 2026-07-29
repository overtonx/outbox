package outbox

import (
	"context"
	"sync"
	"testing"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestCheckpointTracker_AdvancesByContiguousPrefixOnly(t *testing.T) {
	tracker := newCheckpointTracker(0)

	watermark, advanced := tracker.Ack(1)
	assert.True(t, advanced)
	assert.Equal(t, int64(1), watermark)

	watermark, advanced = tracker.Ack(2)
	assert.True(t, advanced)
	assert.Equal(t, int64(2), watermark)

	watermark, advanced = tracker.Ack(3)
	assert.True(t, advanced)
	assert.Equal(t, int64(3), watermark)

	// ack 5 приходит раньше 4 (неупорядоченная доставка) — watermark не
	// должен перепрыгнуть через дырку.
	watermark, advanced = tracker.Ack(5)
	assert.False(t, advanced)
	assert.Equal(t, int64(3), watermark)
	assert.Equal(t, 1, tracker.PendingCount(), "5 должен ждать в acked до прихода 4")

	watermark, advanced = tracker.Ack(4)
	assert.True(t, advanced)
	assert.Equal(t, int64(5), watermark, "4 закрывает дыру, и 5 сразу же подхватывается")
	assert.Equal(t, 0, tracker.PendingCount(), "после продвижения map должна быть пустой — нет утечки")
}

func TestCheckpointTracker_IgnoresStaleAcksAtOrBelowWatermark(t *testing.T) {
	tracker := newCheckpointTracker(10)

	watermark, advanced := tracker.Ack(10)
	assert.False(t, advanced)
	assert.Equal(t, int64(10), watermark)

	watermark, advanced = tracker.Ack(5)
	assert.False(t, advanced)
	assert.Equal(t, int64(10), watermark)
	assert.Equal(t, 0, tracker.PendingCount())
}

func TestCheckpointTracker_ResetDiscardsInMemoryProgressPastGivenWatermark(t *testing.T) {
	tracker := newCheckpointTracker(0)
	tracker.Ack(1)
	tracker.Ack(2)
	tracker.Ack(4) // дыра на 3 — остаётся в pending

	assert.Equal(t, int64(2), tracker.Watermark())
	assert.Equal(t, 1, tracker.PendingCount())

	// Reset имитирует поведение после Recreate/replay: доверяем только
	// последнему персистентному чекпоинту, а не тому, что накопилось в
	// памяти с последнего flush'а.
	tracker.Reset(2)
	assert.Equal(t, int64(2), tracker.Watermark())
	assert.Equal(t, 0, tracker.PendingCount())
}

func TestCheckpointTracker_ConcurrentAcksAreRace_Free(t *testing.T) {
	tracker := newCheckpointTracker(0)

	const n = 2000
	var wg sync.WaitGroup
	wg.Add(n)
	for i := int64(1); i <= n; i++ {
		id := i
		go func() {
			defer wg.Done()
			tracker.Ack(id)
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(n), tracker.Watermark())
	assert.Equal(t, 0, tracker.PendingCount(), "все id пришли — map должна опустеть, утечки нет")
}

func TestCheckpointStore_LoadReturnsZeroWhenNoRowExists(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock_.ExpectQuery("SELECT last_confirmed_id FROM outbox_checkpoints WHERE lock_name = \\?").
		WithArgs("lock-1").
		WillReturnRows(sqlmock.NewRows([]string{"last_confirmed_id"}))

	store := newCheckpointStore(db)
	watermark, err := store.Load(context.Background(), "lock-1")
	assert.NoError(t, err)
	assert.Equal(t, int64(0), watermark)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestCheckpointStore_LoadReturnsPersistedValue(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock_.ExpectQuery("SELECT last_confirmed_id FROM outbox_checkpoints WHERE lock_name = \\?").
		WithArgs("lock-1").
		WillReturnRows(sqlmock.NewRows([]string{"last_confirmed_id"}).AddRow(42))

	store := newCheckpointStore(db)
	watermark, err := store.Load(context.Background(), "lock-1")
	assert.NoError(t, err)
	assert.Equal(t, int64(42), watermark)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestCheckpointStore_SaveUpsertsWithGreatest(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock_.ExpectExec("INSERT INTO outbox_checkpoints .* ON DUPLICATE KEY UPDATE").
		WithArgs("lock-1", int64(7)).
		WillReturnResult(sqlmock.NewResult(0, 1))

	store := newCheckpointStore(db)
	err = store.Save(context.Background(), "lock-1", 7)
	assert.NoError(t, err)
	assert.NoError(t, mock_.ExpectationsWereMet())
}
