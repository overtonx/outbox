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

func TestCleanupService_Cleanup_HappyPath(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	sentRetention := 24 * time.Hour
	dlRetention := 7 * 24 * time.Hour

	service := NewCleanupService(
		mockStore,
		logger,
		nil, // No metrics
		sentRetention,
		dlRetention,
	)

	mockStore.On("DeleteSentEvents", mock.Anything, sentRetention).Return(int64(10), nil).Once()
	mockStore.On("DeleteDeadLetterEvents", mock.Anything, dlRetention).Return(int64(5), nil).Once()

	err := service.Cleanup(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}

func TestCleanupService_Cleanup_StoreFails(t *testing.T) {
	mockStore := new(storage.MockStore)
	logger := zap.NewNop()
	sentRetention := 24 * time.Hour
	dlRetention := 7 * 24 * time.Hour
	storeErr := errors.New("db error")

	service := NewCleanupService(
		mockStore,
		logger,
		nil,
		sentRetention,
		dlRetention,
	)

	mockStore.On("DeleteSentEvents", mock.Anything, sentRetention).Return(int64(0), storeErr).Once()
	mockStore.On("DeleteDeadLetterEvents", mock.Anything, dlRetention).Return(int64(0), storeErr).Once()

	// The cleanup service should not return an error, just log it.
	err := service.Cleanup(context.Background())
	assert.NoError(t, err)

	mockStore.AssertExpectations(t)
}
