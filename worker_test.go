package outbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestBaseWorker_StartAndStop(t *testing.T) {
	workDone := make(chan bool)
	workFunc := func(ctx context.Context) error {
		workDone <- true
		return nil
	}

	worker := NewBaseWorker("test-worker", 20*time.Millisecond, zap.NewNop(), workFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker.Start(ctx)

	// Wait for the worker to do some work
	<-workDone

	// Stop the worker and it should block until shutdown is complete
	worker.Stop()

	// Assert that another piece of work is not done after stopping
	select {
	case <-workDone:
		t.Fatal("work should not have been done after worker was stopped")
	case <-time.After(50 * time.Millisecond):
		// This is expected
	}
}

func TestBaseWorker_ContextCancellation(t *testing.T) {
	var workCounter int32
	workFunc := func(ctx context.Context) error {
		atomic.AddInt32(&workCounter, 1)
		return nil
	}

	worker := NewBaseWorker("test-worker", 20*time.Millisecond, zap.NewNop(), workFunc)

	// Context will be cancelled after 50ms
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	// Start blocks until the worker is stopped
	worker.Start(ctx)

	// After Start() returns, no more work should be done.
	// We read the value once.
	countAfterStop := atomic.LoadInt32(&workCounter)
	assert.Greater(t, countAfterStop, int32(0), "worker should have done some work")

	// Wait a bit to ensure no more work is being done
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, countAfterStop, atomic.LoadInt32(&workCounter), "work should not be done after context is cancelled")
}

func TestBaseWorker_StopIsIdempotent(t *testing.T) {
	workDone := make(chan bool)
	workFunc := func(ctx context.Context) error {
		workDone <- true
		return nil
	}

	worker := NewBaseWorker("test-worker", 20*time.Millisecond, zap.NewNop(), workFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker.Start(ctx)
	<-workDone

	// Call stop multiple times, it should not panic
	worker.Stop()
	worker.Stop()

	assert.NotPanics(t, func() {
		worker.Stop()
	})
}

func TestBaseWorker_StopWaitsForWorkToFinish(t *testing.T) {
	workStarted := make(chan bool, 1)
	workFinished := make(chan bool, 1)

	workFunc := func(ctx context.Context) error {
		workStarted <- true
		// Simulate a long-running task
		time.Sleep(100 * time.Millisecond)
		workFinished <- true
		return nil
	}

	worker := NewBaseWorker("test-worker", 20*time.Millisecond, zap.NewNop(), workFunc)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go worker.Start(ctx)

	// Wait for a piece of work to start
	<-workStarted

	// Call stop. This should block until the long-running task is complete.
	stopCalledTime := time.Now()
	worker.Stop()
	stopFinishedTime := time.Now()

	// Check if stop actually blocked
	assert.True(t, stopFinishedTime.Sub(stopCalledTime) >= 100*time.Millisecond)

	// Check that the work was actually finished
	select {
	case <-workFinished:
		// success
	default:
		t.Fatal("work should have been finished")
	}
}
