package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"go.uber.org/zap"
)

// leaderElector реализует выбор единственного активного лидера среди
// нескольких инстансов Dispatcher, работающих против одной БД, с помощью
// именованного advisory-лока MySQL (GET_LOCK). Только лидер выполняет
// claim/publish/dead-letter цикл; остальные инстансы простаивают как
// hot standby и подхватывают лидерство, если текущий лидер пропадает —
// это и есть гарантия глобального порядка публикации при нескольких
// инстансах: в любой момент времени клеймит и публикует ровно один процесс.
type leaderElector struct {
	db       *sql.DB
	lockName string
	logger   *zap.Logger
}

func newLeaderElector(db *sql.DB, lockName string, logger *zap.Logger) *leaderElector {
	return &leaderElector{db: db, lockName: lockName, logger: logger}
}

// defaultLockName выводит имя лока из имени текущей схемы БД, чтобы
// несколько независимых outbox-развёртываний в разных схемах не мешали
// друг другу, а несколько инстансов одного сервиса в одной схеме — выбирали
// одного лидера.
func defaultLockName(ctx context.Context, db *sql.DB) (string, error) {
	var schema sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&schema); err != nil {
		return "", fmt.Errorf("failed to determine current schema: %w", err)
	}
	if !schema.Valid || schema.String == "" {
		return "outbox:leader", nil
	}
	return "outbox:leader:" + schema.String, nil
}

// run блокируется до отмены ctx, периодически пытаясь получить лидерство.
// Пока текущий процесс лидер, он вызывает onLeader(leaderCtx) в отдельной
// горутине; leaderCtx отменяется, как только лидерство теряется (отмена ctx
// или потеря закреплённого соединения), после чего run ждёт возврата
// onLeader перед освобождением лока и следующей попыткой переизбрания.
func (e *leaderElector) run(ctx context.Context, pollInterval time.Duration, onLeader func(context.Context)) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		conn, acquired, err := e.tryAcquire(ctx)
		if err != nil {
			e.logger.Warn("outbox: leader lock acquisition attempt failed", zap.Error(err))
			continue
		}
		if !acquired {
			continue
		}

		e.holdLeadership(ctx, conn, pollInterval, onLeader)

		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (e *leaderElector) tryAcquire(ctx context.Context) (*sql.Conn, bool, error) {
	conn, err := e.db.Conn(ctx)
	if err != nil {
		return nil, false, err
	}

	var acquired sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", e.lockName).Scan(&acquired); err != nil {
		_ = conn.Close()
		return nil, false, err
	}

	if !acquired.Valid || acquired.Int64 != 1 {
		_ = conn.Close()
		return nil, false, nil
	}

	e.logger.Info("outbox: acquired leader lock", zap.String("lock_name", e.lockName))
	return conn, true, nil
}

// holdLeadership запускает onLeader на весь срок владения локом и
// health-check'ает закреплённое соединение; как только соединение
// потеряно или ctx отменён, отменяет leaderCtx, дожидается возврата
// onLeader и явно освобождает лок.
func (e *leaderElector) holdLeadership(ctx context.Context, conn *sql.Conn, healthCheckInterval time.Duration, onLeader func(context.Context)) {
	leaderCtx, cancel := context.WithCancel(ctx)

	done := make(chan struct{})
	go func() {
		defer close(done)
		onLeader(leaderCtx)
	}()

	healthCheck := time.NewTicker(healthCheckInterval)
	defer healthCheck.Stop()

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-healthCheck.C:
			if err := conn.PingContext(ctx); err != nil {
				e.logger.Warn("outbox: leader connection lost, releasing leadership", zap.Error(err))
				break loop
			}
		}
	}

	cancel()
	<-done
	e.release(conn)
}

func (e *leaderElector) release(conn *sql.Conn) {
	releaseCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := conn.ExecContext(releaseCtx, "SELECT RELEASE_LOCK(?)", e.lockName); err != nil {
		e.logger.Warn("outbox: failed to explicitly release leader lock", zap.Error(err))
	}
	_ = conn.Close()
}
