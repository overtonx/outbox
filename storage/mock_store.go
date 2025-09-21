package storage

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

// MockStore is a mock implementation of the Store interface for testing.
type MockStore struct {
	mock.Mock
}

func (m *MockStore) CreateEvent(ctx context.Context, tx DBTX, event *EventRecord) error {
	args := m.Called(ctx, tx, event)
	return args.Error(0)
}

func (m *MockStore) FetchNewEvents(ctx context.Context, batchSize int) ([]EventRecord, error) {
	args := m.Called(ctx, batchSize)
	return args.Get(0).([]EventRecord), args.Error(1)
}

func (m *MockStore) FetchStuckEvents(ctx context.Context, batchSize int, stuckTimeout time.Duration) ([]EventRecord, error) {
	args := m.Called(ctx, batchSize, stuckTimeout)
	return args.Get(0).([]EventRecord), args.Error(1)
}

func (m *MockStore) FetchEventsToMoveToDeadLetter(ctx context.Context, batchSize int, maxAttempts int) ([]EventRecord, error) {
	args := m.Called(ctx, batchSize, maxAttempts)
	return args.Get(0).([]EventRecord), args.Error(1)
}

func (m *MockStore) MarkAsSent(ctx context.Context, eventID int64) error {
	args := m.Called(ctx, eventID)
	return args.Error(0)
}

func (m *MockStore) MarkAsProcessing(ctx context.Context, eventIDs []int64) error {
	args := m.Called(ctx, eventIDs)
	return args.Error(0)
}

func (m *MockStore) UpdateForRetry(ctx context.Context, eventID int64, nextAttemptAt time.Time, lastError string) error {
	args := m.Called(ctx, eventID, nextAttemptAt, lastError)
	return args.Error(0)
}

func (m *MockStore) MoveToDeadLetter(ctx context.Context, record EventRecord, lastError string) error {
	args := m.Called(ctx, record, lastError)
	return args.Error(0)
}

func (m *MockStore) ResetStuckEvents(ctx context.Context, eventIDs []int64, nextAttemptAt time.Time) error {
	args := m.Called(ctx, eventIDs, nextAttemptAt)
	return args.Error(0)
}

func (m *MockStore) DeleteSentEvents(ctx context.Context, retention time.Duration) (int64, error) {
	args := m.Called(ctx, retention)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockStore) DeleteDeadLetterEvents(ctx context.Context, retention time.Duration) (int64, error) {
	args := m.Called(ctx, retention)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockStore) EnsureTables(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}
