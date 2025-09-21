package outbox

import (
	"context"

	"github.com/stretchr/testify/mock"
)

// MockPublisher is a mock implementation of the Publisher interface.
type MockPublisher struct {
	mock.Mock
}

func (m *MockPublisher) Publish(ctx context.Context, event EventRecord) error {
	args := m.Called(ctx, event)
	return args.Error(0)
}

func (m *MockPublisher) Close() error {
	args := m.Called()
	return args.Error(0)
}
