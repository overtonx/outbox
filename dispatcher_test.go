package outbox

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"
)

// MockWorker is a mock implementation of the Worker interface for testing the Dispatcher.
type MockWorker struct {
	mock.Mock
	startChan chan struct{}
	stopChan  chan struct{}
}

func NewMockWorker() *MockWorker {
	return &MockWorker{
		startChan: make(chan struct{}),
		stopChan:  make(chan struct{}),
	}
}

func (m *MockWorker) Start(ctx context.Context) {
	m.Called(ctx)
	close(m.startChan) // Signal that Start was called
	<-m.stopChan       // Wait until Stop is called
}

func (m *MockWorker) Stop() {
	m.Called()
	close(m.stopChan) // Signal that Stop was called
}

func (m *MockWorker) Name() string {
	args := m.Called()
	return args.String(0)
}

func TestDispatcher_StartAndStop(t *testing.T) {
	logger := zap.NewNop()
	worker1 := NewMockWorker()
	worker2 := NewMockWorker()

	dispatcher := NewDispatcher(logger, worker1, worker2)

	worker1.On("Start", mock.Anything).Return()
	worker1.On("Stop").Return()
	worker2.On("Start", mock.Anything).Return()
	worker2.On("Stop").Return()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatcher.Start(ctx)
	}()

	// Wait for workers to start
	<-worker1.startChan
	<-worker2.startChan

	assert.True(t, dispatcher.IsStarted())

	// Stop the dispatcher
	dispatcher.Stop()

	// Wait for the dispatcher to fully stop
	wg.Wait()

	assert.False(t, dispatcher.IsStarted())
	worker1.AssertExpectations(t)
	worker2.AssertExpectations(t)
}

func TestDispatcher_ContextCancellation(t *testing.T) {
	logger := zap.NewNop()
	worker := NewMockWorker()

	dispatcher := NewDispatcher(logger, worker)

	worker.On("Start", mock.Anything).Return()
	worker.On("Stop").Return()

	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatcher.Start(ctx)
	}()

	// Wait for worker to start
	<-worker.startChan
	assert.True(t, dispatcher.IsStarted())

	// Cancel the context
	cancel()

	// Wait for the dispatcher to fully stop
	wg.Wait()

	assert.False(t, dispatcher.IsStarted())
	worker.AssertExpectations(t)
}
