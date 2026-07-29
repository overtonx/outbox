package outbox

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestDefaultLockName_DerivesFromSchema(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock_.ExpectQuery("SELECT DATABASE\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"database()"}).AddRow("orders_db"))

	name, err := defaultLockName(context.Background(), db)
	assert.NoError(t, err)
	assert.Equal(t, "outbox:leader:orders_db", name)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestDefaultLockName_FallsBackWhenNoSchema(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	mock_.ExpectQuery("SELECT DATABASE\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"database()"}).AddRow(nil))

	name, err := defaultLockName(context.Background(), db)
	assert.NoError(t, err)
	assert.Equal(t, "outbox:leader", name)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestLeaderElector_AcquiresLockAndRunsOnLeader(t *testing.T) {
	db, mock_, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	assert.NoError(t, err)
	defer db.Close()

	mock_.ExpectQuery("SELECT GET_LOCK\\(\\?, 0\\)").
		WithArgs("test-lock").
		WillReturnRows(sqlmock.NewRows([]string{"get_lock"}).AddRow(1))
	// Здоровье закреплённого соединения проверяется каждый healthCheckInterval,
	// пока не истечёт ctx — регистрируем с запасом.
	for i := 0; i < 20; i++ {
		mock_.ExpectPing()
	}
	mock_.ExpectExec("SELECT RELEASE_LOCK\\(\\?\\)").
		WithArgs("test-lock").
		WillReturnResult(sqlmock.NewResult(0, 1))

	elector := newLeaderElector(db, "test-lock", zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()

	leaderRan := make(chan struct{})
	err = elector.run(ctx, 15*time.Millisecond, func(leaderCtx context.Context) {
		close(leaderRan)
		<-leaderCtx.Done()
	})

	assert.ErrorIs(t, err, context.DeadlineExceeded)

	select {
	case <-leaderRan:
	default:
		t.Fatal("expected onLeader to have run")
	}
}

func TestLeaderElector_DoesNotRunOnLeaderWhenLockHeldElsewhere(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	// Регистрируем несколько ответов "лок занят" — за время теста может
	// произойти несколько попыток переизбрания.
	for i := 0; i < 6; i++ {
		mock_.ExpectQuery("SELECT GET_LOCK\\(\\?, 0\\)").
			WithArgs("test-lock").
			WillReturnRows(sqlmock.NewRows([]string{"get_lock"}).AddRow(0))
	}

	elector := newLeaderElector(db, "test-lock", zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
	defer cancel()

	ranLeader := false
	err = elector.run(ctx, 15*time.Millisecond, func(context.Context) {
		ranLeader = true
	})

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.False(t, ranLeader, "onLeader must not run when the lock is held elsewhere")
}
