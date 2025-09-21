package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/storage"
)

func TestEventProcessor_ProcessEvents_HappyPath(t *testing.T) {
	mockStore := new(storage.MockStore)
	mockPublisher := new(MockPublisher)
	logger := zap.NewNop()

	processor := NewEventProcessor(
		mockStore,
		mockPublisher,
		logger,
		nil, // No metrics
		DefaultBackoffStrategy(),
		3, // maxAttempts
		10, // batchSize
	)

	events := []storage.EventRecord{{ID: 1, Topic: "test-topic"}}
	eventIDs := []int64{1}

	mockStore.On("FetchNewEvents", mock.Anything, 10).Return(events, nil).Once()
	mockStore.On("MarkAsProcessing", mock.Anything, eventIDs).Return(nil).Once()
	mockPublisher.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()
	mockStore.On("MarkAsSent", mock.Anything, int64(1)).Return(nil).Once()

	err := processor.ProcessEvents(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestEventProcessor_ProcessEvents_NoEvents(t *testing.T) {
	mockStore := new(storage.MockStore)
	mockPublisher := new(MockPublisher)
	logger := zap.NewNop()

	processor := NewEventProcessor(
		mockStore,
		mockPublisher,
		logger,
		nil,
		DefaultBackoffStrategy(),
		3,
		10,
	)

	mockStore.On("FetchNewEvents", mock.Anything, 10).Return([]storage.EventRecord{}, nil).Once()

	err := processor.ProcessEvents(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
	mockPublisher.AssertNotCalled(t, "Publish")
}

func TestEventProcessor_ProcessEvents_PublishFails_Retry(t *testing.T) {
	mockStore := new(storage.MockStore)
	mockPublisher := new(MockPublisher)
	logger := zap.NewNop()

	processor := NewEventProcessor(
		mockStore,
		mockPublisher,
		logger,
		nil,
		DefaultBackoffStrategy(),
		3,
		10,
	)

	events := []storage.EventRecord{{ID: 1, Topic: "test-topic", AttemptCount: 0}}
	eventIDs := []int64{1}
	publishErr := errors.New("kafka is down")

	mockStore.On("FetchNewEvents", mock.Anything, 10).Return(events, nil).Once()
	mockStore.On("MarkAsProcessing", mock.Anything, eventIDs).Return(nil).Once()
	mockPublisher.On("Publish", mock.Anything, mock.Anything).Return(publishErr).Once()
	mockStore.On("UpdateForRetry", mock.Anything, int64(1), mock.AnythingOfType("time.Time"), publishErr.Error()).Return(nil).Once()

	err := processor.ProcessEvents(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestEventProcessor_ProcessEvents_PublishFails_MaxAttempts(t *testing.T) {
	mockStore := new(storage.MockStore)
	mockPublisher := new(MockPublisher)
	logger := zap.NewNop()

	maxAttempts := 3
	processor := NewEventProcessor(
		mockStore,
		mockPublisher,
		logger,
		nil,
		DefaultBackoffStrategy(),
		maxAttempts,
		10,
	)

	events := []storage.EventRecord{{ID: 1, Topic: "test-topic", AttemptCount: maxAttempts - 1}}
	eventIDs := []int64{1}
	publishErr := errors.New("kafka is still down")

	mockStore.On("FetchNewEvents", mock.Anything, 10).Return(events, nil).Once()
	mockStore.On("MarkAsProcessing", mock.Anything, eventIDs).Return(nil).Once()
	mockPublisher.On("Publish", mock.Anything, mock.Anything).Return(publishErr).Once()
	// Should be marked for DLQ
	mockStore.On("UpdateForRetry", mock.Anything, int64(1), mock.AnythingOfType("time.Time"), publishErr.Error()).Return(nil).Once()


	err := processor.ProcessEvents(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}

func TestEventProcessor_ProcessEvents_StoreFailsOnUpdate(t *testing.T) {
	mockStore := new(storage.MockStore)
	mockPublisher := new(MockPublisher)
	logger := zap.NewNop()

	processor := NewEventProcessor(
		mockStore,
		mockPublisher,
		logger,
		nil,
		DefaultBackoffStrategy(),
		3,
		10,
	)

	events := []storage.EventRecord{{ID: 1, Topic: "test-topic"}}
	eventIDs := []int64{1}
	storeErr := errors.New("db connection lost")

	mockStore.On("FetchNewEvents", mock.Anything, 10).Return(events, nil).Once()
	mockStore.On("MarkAsProcessing", mock.Anything, eventIDs).Return(nil).Once()
	mockPublisher.On("Publish", mock.Anything, mock.Anything).Return(nil).Once()
	mockStore.On("MarkAsSent", mock.Anything, int64(1)).Return(storeErr).Once()

	err := processor.ProcessEvents(context.Background())
	assert.NoError(t, err) // The main loop should not fail, just the event processing

	mockStore.AssertExpectations(t)
	mockPublisher.AssertExpectations(t)
}
