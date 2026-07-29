package outbox

import (
	"context"
	"database/sql"
	"sync"
)

// checkpointTracker отслеживает подтверждения (ack) упорядоченного потока и
// продвигает watermark строго по непрерывному префиксу: ack 1,2,3,5 даёт
// watermark=3, а 5 остаётся в acked до тех пор, пока не придёт 4 (или пока
// Reset не отбросит его при replay). Все операции потокобезопасны — Ack
// вызывается из горутины-обработчика Events() продюсера, а Watermark/Reset —
// из горутины оркестрации Dispatcher'а.
type checkpointTracker struct {
	mu        sync.Mutex
	watermark int64
	acked     map[int64]struct{}
}

func newCheckpointTracker(initial int64) *checkpointTracker {
	return &checkpointTracker{
		watermark: initial,
		acked:     make(map[int64]struct{}),
	}
}

// Ack помечает id подтверждённым и продвигает watermark настолько далеко,
// насколько позволяет непрерывность подтверждений. id <= текущего watermark
// игнорируется (устаревший ack, например пришедший после Reset).
func (t *checkpointTracker) Ack(id int64) (watermark int64, advanced bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if id <= t.watermark {
		return t.watermark, false
	}

	t.acked[id] = struct{}{}
	for {
		next := t.watermark + 1
		if _, ok := t.acked[next]; !ok {
			break
		}
		delete(t.acked, next)
		t.watermark = next
		advanced = true
	}
	return t.watermark, advanced
}

// Watermark возвращает последний подтверждённый непрерывный id.
func (t *checkpointTracker) Watermark() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.watermark
}

// Reset отбрасывает все непрерывные подтверждения, накопленные с последнего
// персистентного чекпоинта, и переустанавливает watermark на переданное
// значение. Используется после пересоздания producer'а: состояние до сбоя
// не может считаться надёжным (сообщения могли не дойти), поэтому
// единственный источник истины для возобновления — последний персистентный
// чекпоинт, а не то, что успело накопиться в памяти.
func (t *checkpointTracker) Reset(watermark int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.watermark = watermark
	t.acked = make(map[int64]struct{})
}

// PendingCount возвращает число подтверждений, ожидающих продолжения
// непрерывного префикса — используется в тестах для проверки отсутствия
// утечки map при неупорядоченном приходе ack-ов.
func (t *checkpointTracker) PendingCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.acked)
}

// checkpointStore персистит watermark упорядоченного потока в
// outbox_checkpoints — по одной строке на lockName (тот же lockName, что
// используется для leader-election), чтобы независимые outbox-каналы не
// делили один чекпоинт.
type checkpointStore struct {
	db *sql.DB
}

func newCheckpointStore(db *sql.DB) *checkpointStore {
	return &checkpointStore{db: db}
}

// Load возвращает последний персистентный watermark для lockName, либо 0,
// если чекпоинт ещё ни разу не сохранялся (поток начинается с начала).
func (s *checkpointStore) Load(ctx context.Context, lockName string) (int64, error) {
	var watermark int64
	err := s.db.QueryRowContext(ctx,
		"SELECT last_confirmed_id FROM outbox_checkpoints WHERE lock_name = ?", lockName,
	).Scan(&watermark)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return watermark, nil
}

// Save сохраняет watermark, если он больше уже сохранённого — GREATEST
// защищает от отката персистентного чекпоинта назад, если запись из
// предыдущего (более медленного) флаша придёт после более свежей.
func (s *checkpointStore) Save(ctx context.Context, lockName string, watermark int64) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO outbox_checkpoints (lock_name, last_confirmed_id, updated_at)
		VALUES (?, ?, NOW(6))
		ON DUPLICATE KEY UPDATE
			last_confirmed_id = GREATEST(last_confirmed_id, VALUES(last_confirmed_id)),
			updated_at = VALUES(updated_at)
	`, lockName, watermark)
	return err
}
