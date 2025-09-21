package outbox

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/require"
)

func TestNewOutboxEvent(t *testing.T) {
	payload := struct {
		Name string `json:"name"`
	}{
		Name: "test name",
	}

	event, err := NewOutboxEvent("1", "test", "event", "2", "topic", payload, map[string]string{})
	require.NoError(t, err)

	jsonRaw, err := json.Marshal(event.Payload)
	require.NoError(t, err)
	require.Equal(t, string(jsonRaw), `{"name":"test name"}`)

}

func TestSaveEvent(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	mock.ExpectBegin()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)

	event, err := NewOutboxEvent(
		"test-id-123",
		"UserCreated",
		"User",
		"user-abc",
		"user-events",
		map[string]string{"email": "test@example.com"},
		map[string]string{"source": "test"},
	)
	require.NoError(t, err)

	payloadJSON, _ := json.Marshal(event.Payload)
	headersJSON, _ := json.Marshal(event.Headers)

	mock.ExpectExec("INSERT INTO outbox_events").
		WithArgs(event.EventID, event.EventType, event.AggregateType, event.AggregateID, event.Topic, payloadJSON, headersJSON, EventStatusNew).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = SaveEvent(ctx, tx, event)
	require.NoError(t, err)

	mock.ExpectCommit()
	err = tx.Commit()
	require.NoError(t, err)

	err = mock.ExpectationsWereMet()
	require.NoError(t, err, "expectations were not met")
}
