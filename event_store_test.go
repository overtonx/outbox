package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestEventStore_Save_Success(t *testing.T) {
	ctx := context.Background()
	executor := new(MockDBExecutor)
	executor.On("ExecContext", ctx, mock.Anything, mock.Anything).Return(nil, nil).Once()

	store := NewEventStore(JSONSerializer{})
	event := Event{
		EventType:     "order.created",
		AggregateType: "order",
		AggregateID:   "order-1",
		Topic:         "orders",
		Payload:       map[string]string{"id": "order-1"},
	}

	err := store.Save(ctx, executor, event)

	assert.NoError(t, err)
	executor.AssertExpectations(t)
}

func TestEventStore_Save_AutoGeneratesEventID(t *testing.T) {
	ctx := context.Background()
	executor := new(MockDBExecutor)

	var capturedArgs []interface{}
	executor.On("ExecContext", ctx, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedArgs = args.Get(2).([]interface{})
		}).
		Return(nil, nil).Once()

	store := NewEventStore(JSONSerializer{})
	event := Event{
		EventType:     "order.created",
		AggregateType: "order",
		AggregateID:   "order-1",
		Topic:         "orders",
		Payload:       "payload",
	}

	err := store.Save(ctx, executor, event)

	assert.NoError(t, err)
	// First arg in ExecContext is the event_id — must be non-empty
	assert.NotEmpty(t, capturedArgs[0], "event_id should be auto-generated")
	executor.AssertExpectations(t)
}

func TestEventStore_Save_SerializationError(t *testing.T) {
	ctx := context.Background()
	executor := new(MockDBExecutor)

	store := NewEventStore(JSONSerializer{})
	event := Event{
		EventType:     "order.created",
		AggregateType: "order",
		AggregateID:   "order-1",
		Topic:         "orders",
		Payload:       make(chan int), // channels cannot be JSON-serialized
	}

	err := store.Save(ctx, executor, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to serialize payload")
	executor.AssertNotCalled(t, "ExecContext")
}

func TestEventStore_Save_ValidationError(t *testing.T) {
	ctx := context.Background()
	executor := new(MockDBExecutor)
	store := NewEventStore(JSONSerializer{})

	cases := []struct {
		name    string
		event   Event
		wantMsg string
	}{
		{
			"missing aggregate_type",
			Event{AggregateID: "1", Topic: "t", Payload: "p"},
			"aggregate_type is required",
		},
		{
			"missing aggregate_id",
			Event{AggregateType: "t", Topic: "t", Payload: "p"},
			"aggregate_id is required",
		},
		{
			"missing topic",
			Event{AggregateType: "t", AggregateID: "1", Payload: "p"},
			"topic is required",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := store.Save(ctx, executor, tc.event)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantMsg)
		})
	}
	executor.AssertNotCalled(t, "ExecContext")
}

func TestEventStore_Save_DuplicateKeyError(t *testing.T) {
	ctx := context.Background()
	executor := new(MockDBExecutor)
	executor.On("ExecContext", ctx, mock.Anything, mock.Anything).
		Return(nil, &mysql.MySQLError{Number: 1062}).Once()

	store := NewEventStore(JSONSerializer{})
	event := Event{
		EventID:       "dup-id",
		EventType:     "order.created",
		AggregateType: "order",
		AggregateID:   "order-1",
		Topic:         "orders",
		Payload:       "payload",
	}

	err := store.Save(ctx, executor, event)

	assert.Error(t, err)
	assert.True(t, errors.Is(err, ErrEventAlreadyExists))
	executor.AssertExpectations(t)
}

func TestEventStore_Save_GenericDBError(t *testing.T) {
	ctx := context.Background()
	executor := new(MockDBExecutor)
	executor.On("ExecContext", ctx, mock.Anything, mock.Anything).
		Return(nil, errors.New("connection reset")).Once()

	store := NewEventStore(JSONSerializer{})
	event := Event{
		EventID:       "evt-1",
		EventType:     "order.created",
		AggregateType: "order",
		AggregateID:   "order-1",
		Topic:         "orders",
		Payload:       "payload",
	}

	err := store.Save(ctx, executor, event)

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to save outbox event")
	assert.False(t, errors.Is(err, ErrEventAlreadyExists))
	executor.AssertExpectations(t)
}

func TestEventStore_Save_ContentTypeInQuery(t *testing.T) {
	ctx := context.Background()
	executor := new(MockDBExecutor)

	var capturedQuery string
	var capturedArgs []interface{}
	executor.On("ExecContext", ctx, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedQuery = args.Get(1).(string)
			capturedArgs = args.Get(2).([]interface{})
		}).
		Return(nil, nil).Once()

	store := NewEventStore(JSONSerializer{})
	event := Event{
		EventID:       "evt-1",
		EventType:     "order.created",
		AggregateType: "order",
		AggregateID:   "order-1",
		Topic:         "orders",
		Payload:       "payload",
	}

	err := store.Save(ctx, executor, event)

	assert.NoError(t, err)
	assert.Contains(t, capturedQuery, "content_type")
	// content_type arg should be "application/json"
	assert.Equal(t, ContentTypeJSON, capturedArgs[5])
}
