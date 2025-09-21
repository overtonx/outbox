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

func TestStuckEventService_RecoverStuckEvents_HappyPath(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	stuckTimeout := 10 * time.Minute
	batchSize := 10

	service := NewStuckEventService(
		mockStore,
		logger,
		nil, // No metrics
		DefaultBackoffStrategy(),
		batchSize,
		stuckTimeout,
	)

	events := []storage.EventRecord{
		{ID: 1, Status: storage.StatusProcessing},
		{ID: 2, Status: storage.StatusProcessing},
	}
	eventIDs := []int64{1, 2}

	mockStore.On("FetchStuckEvents", mock.Anything, batchSize, stuckTimeout).Return(events, nil).Once()
	mockStore.On("ResetStuckEvents", mock.Anything, eventIDs, mock.AnythingOfType("time.Time")).Return(nil).Once()

	err := service.RecoverStuckEvents(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}

func TestStuckEventService_RecoverStuckEvents_NoEvents(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	stuckTimeout := 10 * time.Minute
	batchSize := 10

	service := NewStuckEventService(
		mockStore,
		logger,
		nil,
		DefaultBackoffStrategy(),
		batchSize,
		stuckTimeout,
	)

	mockStore.On("FetchStuckEvents", mock.Anything, batchSize, stuckTimeout).Return([]storage.EventRecord{}, nil).Once()

	err := service.RecoverStuckEvents(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
	mockStore.AssertNotCalled(t, "ResetStuckEvents")
}

func TestStuckEventService_RecoverStuckEvents_StoreFetchFails(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	stuckTimeout := 10 * time.Minute
	batchSize := 10
	storeErr := errors.New("db is down")

	service := NewStuckEventService(
		mockStore,
		logger,
		nil,
		DefaultBackoffStrategy(),
		batchSize,
		stuckTimeout,
	)

	mockStore.On("FetchStuckEvents", mock.Anything, batchSize, stuckTimeout).Return(nil, storeErr).Once()

	err := service.RecoverStuckEvents(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "failed to fetch stuck events: db is down", err.Error())

	mockStore.AssertExpectations(t)
}

func TestStuckEventService_RecoverStuckEvents_StoreResetFails(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	stuckTimeout := 10 * time.Minute
	batchSize := 10
	storeErr := errors.New("failed to reset")

	service := NewStuckEventService(
		mockStore,
		logger,
		nil,
		DefaultBackoffStrategy(),
		batchSize,
		stuckTimeout,
	)

	events := []storage.EventRecord{{ID: 1, Status: storage.StatusProcessing}}
	eventIDs := []int64{1}

	mockStore.On("FetchStuckEvents", mock.Anything, batchSize, stuckTimeout).Return(events, nil).Once()
	mockStore.On("ResetStuckEvents", mock.Anything, eventIDs, mock.AnythingOfType("time.Time")).Return(storeErr).Once()

	err := service.RecoverStuckEvents(context.Background())
	assert.Error(t, err)
	assert.Equal(t, storeErr, err)

	mockStore.AssertExpectations(t)
}
