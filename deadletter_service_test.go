package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/overtonx/outbox/v2/storage"
)

func TestDeadLetterService_MoveToDeadLetters_HappyPath(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	maxAttempts := 5
	batchSize := 10

	service := NewDeadLetterService(
		mockStore,
		logger,
		nil, // No metrics
		batchSize,
		maxAttempts,
	)

	events := []storage.EventRecord{
		{ID: 1, AttemptCount: maxAttempts},
		{ID: 2, AttemptCount: maxAttempts + 1},
	}

	mockStore.On("FetchEventsToMoveToDeadLetter", mock.Anything, batchSize, maxAttempts).Return(events, nil).Once()
	mockStore.On("MoveToDeadLetter", mock.Anything, events[0], mock.AnythingOfType("string")).Return(nil).Once()
	mockStore.On("MoveToDeadLetter", mock.Anything, events[1], mock.AnythingOfType("string")).Return(nil).Once()

	err := service.MoveToDeadLetters(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}

func TestDeadLetterService_MoveToDeadLetters_NoEvents(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	maxAttempts := 5
	batchSize := 10

	service := NewDeadLetterService(
		mockStore,
		logger,
		nil,
		batchSize,
		maxAttempts,
	)

	mockStore.On("FetchEventsToMoveToDeadLetter", mock.Anything, batchSize, maxAttempts).Return([]storage.EventRecord{}, nil).Once()

	err := service.MoveToDeadLetters(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
	mockStore.AssertNotCalled(t, "MoveToDeadLetter")
}

func TestDeadLetterService_MoveToDeadLetters_StoreFetchFails(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	maxAttempts := 5
	batchSize := 10
	storeErr := errors.New("db is down")

	service := NewDeadLetterService(
		mockStore,
		logger,
		nil,
		batchSize,
		maxAttempts,
	)

	mockStore.On("FetchEventsToMoveToDeadLetter", mock.Anything, batchSize, maxAttempts).Return(nil, storeErr).Once()

	err := service.MoveToDeadLetters(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "failed to fetch events for dead-letter queue: db is down", err.Error())

	mockStore.AssertExpectations(t)
}

func TestDeadLetterService_MoveToDeadLetters_StoreMoveFails(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	maxAttempts := 5
	batchSize := 10
	storeErr := errors.New("failed to move")

	service := NewDeadLetterService(
		mockStore,
		logger,
		nil,
		batchSize,
		maxAttempts,
	)

	events := []storage.EventRecord{{ID: 1, AttemptCount: maxAttempts}}

	mockStore.On("FetchEventsToMoveToDeadLetter", mock.Anything, batchSize, maxAttempts).Return(events, nil).Once()
	mockStore.On("MoveToDeadLetter", mock.Anything, events[0], mock.AnythingOfType("string")).Return(storeErr).Once()

	err := service.MoveToDeadLetters(context.Background())
	assert.NoError(t, err) // The service logs the error but doesn't fail the whole worker

	mockStore.AssertExpectations(t)
}
