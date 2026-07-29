package outbox

import (
	"context"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestClaimBatch_QueriesAllThreeStatusBranches(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	d := &Dispatcher{
		db:                     db,
		logger:                 zap.NewNop(),
		batchSize:              10,
		processingLeaseTimeout: 60 * time.Second,
	}

	rows := sqlmock.NewRows([]string{
		"id", "event_id", "event_type", "aggregate_type", "aggregate_id", "topic",
		"content_type", "payload", "headers", "attempt_count", "next_attempt_at",
	}).AddRow(1, "evt-1", "order.created", "order", "order-1", "orders",
		"application/json", []byte(`{}`), nil, 0, nil)

	mock_.ExpectBegin()
	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*FOR UPDATE SKIP LOCKED").
		WithArgs(EventRecordStatusNew, EventRecordStatusRetry, EventRecordStatusProcessing, 60, 10).
		WillReturnRows(rows)
	mock_.ExpectExec("UPDATE outbox_events SET status = \\?, updated_at = NOW\\(6\\) WHERE id IN").
		WithArgs(EventRecordStatusProcessing, int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock_.ExpectCommit()

	events, err := d.claimBatch(context.Background())
	assert.NoError(t, err)
	assert.Len(t, events, 1)
	assert.Equal(t, "evt-1", events[0].EventID)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestClaimBatch_EmptyResultDoesNotUpdate(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	d := &Dispatcher{
		db:                     db,
		logger:                 zap.NewNop(),
		batchSize:              10,
		processingLeaseTimeout: 60 * time.Second,
	}

	rows := sqlmock.NewRows([]string{
		"id", "event_id", "event_type", "aggregate_type", "aggregate_id", "topic",
		"content_type", "payload", "headers", "attempt_count", "next_attempt_at",
	})

	mock_.ExpectBegin()
	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*FOR UPDATE SKIP LOCKED").
		WillReturnRows(rows)
	mock_.ExpectCommit()

	events, err := d.claimBatch(context.Background())
	assert.NoError(t, err)
	assert.Empty(t, events)
	assert.NoError(t, mock_.ExpectationsWereMet())
}

func TestClaimBatch_QueryErrorRollsBack(t *testing.T) {
	db, mock_, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	d := &Dispatcher{
		db:                     db,
		logger:                 zap.NewNop(),
		batchSize:              10,
		processingLeaseTimeout: 60 * time.Second,
	}

	mock_.ExpectBegin()
	mock_.ExpectQuery("SELECT (.|\n)*FROM outbox_events(.|\n)*FOR UPDATE SKIP LOCKED").
		WillReturnError(assert.AnError)
	mock_.ExpectRollback()

	events, err := d.claimBatch(context.Background())
	assert.Error(t, err)
	assert.Nil(t, events)
	assert.NoError(t, mock_.ExpectationsWereMet())
}
