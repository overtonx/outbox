package outbox

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// mockWorker is a more realistic mock implementation of the Worker interface.
type mockWorker struct {
	name        string
	startCalled chan bool
	stopCalled  chan bool
	stopChan    chan struct{}
	wg          sync.WaitGroup
}

func newMockWorker(name string) *mockWorker {
	return &mockWorker{
		name:        name,
		startCalled: make(chan bool, 1),
		stopCalled:  make(chan bool, 1),
		stopChan:    make(chan struct{}),
	}
}

func (m *mockWorker) Name() string {
	return m.name
}

func (m *mockWorker) Start(ctx context.Context) {
	m.wg.Add(1)
	defer m.wg.Done()
	m.startCalled <- true

	select {
	case <-ctx.Done():
	case <-m.stopChan:
	}
}

func (m *mockWorker) Stop() {
	m.stopCalled <- true
	close(m.stopChan)
	m.wg.Wait() // Wait for the Start loop to exit.
}

func TestDispatcher_StartAndStop(t *testing.T) {
	logger, _ := zap.NewDevelopment()
	worker1 := newMockWorker("worker1")
	worker2 := newMockWorker("worker2")

	dispatcher := NewDispatcher(logger, worker1, worker2)

	assert.False(t, dispatcher.IsStarted(), "Dispatcher should not be started initially")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		dispatcher.Start(ctx)
	}()

	// Verify that Start() was called on both workers
	select {
	case <-worker1.startCalled:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("worker1.Start was not called")
	}
	select {
	case <-worker2.startCalled:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("worker2.Start was not called")
	}

	assert.True(t, dispatcher.IsStarted(), "Dispatcher should be in started state")

	// Now, stop the dispatcher
	dispatcher.Stop()

	// Verify that Stop() was called on both workers
	select {
	case <-worker1.stopCalled:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("worker1.Stop was not called")
	}
	select {
	case <-worker2.stopCalled:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("worker2.Stop was not called")
	}

	wg.Wait() // Wait for the dispatcher's Start routine to exit

	assert.False(t, dispatcher.IsStarted(), "Dispatcher should be in stopped state after Stop()")
}

func TestDispatcher_ContextCancellation(t *testing.T) {
	logger := zap.NewNop()
	worker := newMockWorker("test-worker")
	dispatcher := NewDispatcher(logger, worker)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	dispatcher.Start(ctx)

	// Verify that Stop() was called on the worker due to context cancellation
	select {
	case <-worker.stopCalled:
		// success
	case <-time.After(1 * time.Second):
		t.Fatal("worker.Stop was not called after context cancellation")
	}
}

func TestDispatcher_MultipleStartAndStop(t *testing.T) {
	logger := zap.NewNop()
	worker := newMockWorker("test-worker")
	dispatcher := NewDispatcher(logger, worker)

	// Start once
	ctx, cancel := context.WithCancel(context.Background())
	go dispatcher.Start(ctx)
	<-worker.startCalled
	assert.True(t, dispatcher.IsStarted())

	// Try starting again
	dispatcher.Start(ctx) // Should be a no-op and log a warning
	assert.True(t, dispatcher.IsStarted())

	// Stop once
	dispatcher.Stop()
	<-worker.stopCalled
	// We need to wait for the Start goroutine to finish
	time.Sleep(10 * time.Millisecond)
	assert.False(t, dispatcher.IsStarted())

	// Try stopping again
	dispatcher.Stop() // Should be a no-op
	assert.False(t, dispatcher.IsStarted())

	cancel()
}
