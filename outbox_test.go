package outbox

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/overtonx/outbox/v2/storage"
)

func TestNewOutboxEvent(t *testing.T) {
	t.Run("should create event successfully", func(t *testing.T) {
		payload := map[string]string{"name": "test name"}
		event, err := NewOutboxEvent("event1", "test.event", "user", "user1", "user-events", payload, nil)

		require.NoError(t, err)
		assert.Equal(t, "event1", event.EventID)
		assert.Equal(t, "user-events", event.Topic)
	})

	t.Run("should return error for invalid event", func(t *testing.T) {
		_, err := NewOutboxEvent("", "", "", "", "", nil, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "aggregate_type is required")
	})
}

func TestSaveEvent(t *testing.T) {
	mockStore := new(storage.MockStore)
	ctx := context.Background()
	// A nil DBTX is fine for this test since we're mocking the store call.
	var tx storage.DBTX

	payload := map[string]string{"name": "test name"}
	event, err := NewOutboxEvent("event1", "test.event", "user", "user1", "user-events", payload, nil)
	require.NoError(t, err)

	mockStore.On("CreateEvent", ctx, tx, mock.MatchedBy(func(r *storage.EventRecord) bool {
		return r.EventID == "event1" && r.Topic == "user-events"
	})).Return(nil).Once()

	err = SaveEvent(ctx, mockStore, tx, event)
	require.NoError(t, err)

	mockStore.AssertExpectations(t)
}

func TestSaveEvent_ValidationFails(t *testing.T) {
	mockStore := new(storage.MockStore)
	ctx := context.Background()
	var tx storage.DBTX

	// Invalid event (missing topic)
	event, err := NewOutboxEvent("event1", "test.event", "user", "user1", "", nil, nil)
	require.Error(t, err) // Error should be caught at creation

	// Even if an invalid event is passed to SaveEvent, it should fail
	invalidEvent := Event{AggregateID: "1"} // Missing other required fields
	err = SaveEvent(ctx, mockStore, tx, invalidEvent)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validation failed")

	mockStore.AssertNotCalled(t, "CreateEvent")
}
