package outbox

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockDBExecutor struct {
	mock.Mock
}

func (m *MockDBExecutor) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	arguments := m.Called(ctx, query, args)
	res, _ := arguments.Get(0).(sql.Result)
	return res, arguments.Error(1)
}

func TestSaveEvent(t *testing.T) {
	ctx := context.Background()
	mockExecutor := new(MockDBExecutor)

	baseEvent := Event{
		EventType:     "test_event",
		AggregateType: "test_aggregate",
		AggregateID:   "agg_id_123",
		Topic:         "test_topic",
		Payload:       map[string]string{"data": "some_data"},
		Headers:       map[string]string{"source": "test"},
	}

	t.Run("success", func(t *testing.T) {
		event := baseEvent
		event.EventID = uuid.NewString()
		mockExecutor.On("ExecContext", ctx, mock.Anything, mock.Anything).Return(nil, nil).Once()

		err := SaveEvent(ctx, mockExecutor, event)

		assert.NoError(t, err)
		mockExecutor.AssertExpectations(t)
	})

	t.Run("success with generated event_id", func(t *testing.T) {
		event := baseEvent
		event.EventID = "" // ID should be generated
		mockExecutor.On("ExecContext", ctx, mock.Anything, mock.Anything).Return(nil, nil).Once()

		err := SaveEvent(ctx, mockExecutor, event)

		assert.NoError(t, err)
		mockExecutor.AssertExpectations(t)
	})

	t.Run("validation failed", func(t *testing.T) {
		testCases := []struct {
			name    string
			event   Event
			wantErr string
		}{
			{"missing aggregate type", Event{EventID: "1", AggregateID: "1", Topic: "t"}, "validation failed: aggregate_type is required"},
			{"missing aggregate id", Event{EventID: "1", AggregateType: "t", Topic: "t"}, "validation failed: aggregate_id is required"},
			{"missing topic", Event{EventID: "1", AggregateType: "t", AggregateID: "1"}, "validation failed: topic is required"},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				err := SaveEvent(ctx, mockExecutor, tc.event)
				assert.Error(t, err)
				assert.Equal(t, tc.wantErr, err.Error())
			})
		}
	})

	t.Run("insert returns duplicate error", func(t *testing.T) {
		event := baseEvent
		event.EventID = uuid.NewString()
		dbErr := &mysql.MySQLError{Number: 1062}
		mockExecutor.On("ExecContext", ctx, mock.Anything, mock.Anything).Return(nil, dbErr).Once()

		err := SaveEvent(ctx, mockExecutor, event)

		assert.Error(t, err)
		assert.True(t, errors.Is(err, ErrEventAlreadyExists))
		mockExecutor.AssertExpectations(t)
	})

	t.Run("insert returns generic db error", func(t *testing.T) {
		event := baseEvent
		event.EventID = uuid.NewString()
		dbErr := errors.New("some db error")
		mockExecutor.On("ExecContext", ctx, mock.Anything, mock.Anything).Return(nil, dbErr).Once()

		err := SaveEvent(ctx, mockExecutor, event)

		assert.Error(t, err)
		assert.False(t, errors.Is(err, ErrEventAlreadyExists))
		assert.Contains(t, err.Error(), "failed to save outbox event")
		mockExecutor.AssertExpectations(t)
	})

	t.Run("payload marshal error", func(t *testing.T) {
		event := baseEvent
		event.EventID = uuid.NewString()
		// Use a channel, which cannot be marshaled to JSON
		event.Payload = make(chan int)

		err := SaveEvent(ctx, mockExecutor, event)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to marshal payload")
	})
}
