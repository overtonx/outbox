package outbox

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

// Dispatcher manages the lifecycle of a collection of workers.
// It is responsible for starting and stopping them gracefully.
type Dispatcher struct {
	logger *zap.Logger
	wg     sync.WaitGroup

	mu       sync.RWMutex
	workers  []Worker
	stopOnce sync.Once
	stopChan chan struct{}
	started  bool
}

// NewDispatcher creates a new dispatcher to manage the given workers.
func NewDispatcher(logger *zap.Logger, workers ...Worker) *Dispatcher {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Dispatcher{
		logger:   logger,
		workers:  workers,
		stopChan: make(chan struct{}),
	}
}

// Start runs all the workers and blocks until the context is cancelled or Stop() is called.
func (d *Dispatcher) Start(ctx context.Context) {
	d.mu.Lock()
	if d.started {
		d.mu.Unlock()
		d.logger.Warn("Dispatcher already started")
		return
	}
	d.started = true
	d.mu.Unlock()

	d.logger.Info("Starting dispatcher with workers", zap.Int("worker_count", len(d.workers)))

	for _, w := range d.workers {
		d.wg.Add(1)
		go func(worker Worker) {
			defer d.wg.Done()
			d.logger.Info("Starting worker", zap.String("worker_name", worker.Name()))
			worker.Start(ctx)
			d.logger.Info("Worker stopped", zap.String("worker_name", worker.Name()))
		}(w)
	}

	// Wait for context cancellation or an explicit stop signal.
	select {
	case <-ctx.Done():
		d.logger.Info("Context cancelled, stopping dispatcher")
		d.Stop() // Ensure stop logic is triggered
	case <-d.stopChan:
		d.logger.Info("Stop signal received, stopping dispatcher")
	}

	// Wait for all workers to finish their shutdown.
	d.wg.Wait()
	d.logger.Info("All workers have been stopped. Dispatcher shutdown complete.")

	d.mu.Lock()
	d.started = false
	d.mu.Unlock()
}

// Stop gracefully shuts down the dispatcher and all its workers.
// It is safe to call Stop multiple times.
func (d *Dispatcher) Stop() {
	d.stopOnce.Do(func() {
		d.mu.RLock()
		defer d.mu.RUnlock()
		if !d.started {
			d.logger.Warn("Attempted to stop a dispatcher that was not started")
			return
		}
		d.logger.Info("Stopping dispatcher...")
		close(d.stopChan)

		// Also stop individual workers. This is important for the worker's own graceful shutdown.
		for _, worker := range d.workers {
			worker.Stop()
		}
	})
}

// IsStarted returns true if the dispatcher is currently running.
func (d *Dispatcher) IsStarted() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.started
}
