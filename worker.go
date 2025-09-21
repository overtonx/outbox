package outbox

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// BaseWorker is a generic, ticker-based worker implementation.
// It runs a given function at a specified interval and handles graceful shutdown.
type BaseWorker struct {
	name     string
	interval time.Duration
	logger   *zap.Logger
	workFunc func(ctx context.Context) error

	wg       sync.WaitGroup
	mu       sync.RWMutex
	stopOnce sync.Once
	stopChan chan struct{}
	started  bool
}

// NewBaseWorker creates a new generic worker.
func NewBaseWorker(name string, interval time.Duration, logger *zap.Logger, workFunc func(ctx context.Context) error) *BaseWorker {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &BaseWorker{
		name:     name,
		interval: interval,
		logger:   logger,
		workFunc: workFunc,
		stopChan: make(chan struct{}),
	}
}

// Start begins the worker's execution loop.
// It blocks until the worker is stopped via the context or a call to Stop().
func (w *BaseWorker) Start(ctx context.Context) {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		w.logger.Warn("Worker already started", zap.String("name", w.name))
		return
	}
	w.started = true
	w.mu.Unlock()

	w.logger.Info("Worker starting", zap.String("name", w.name), zap.Duration("interval", w.interval))
	defer w.logger.Info("Worker finished", zap.String("name", w.name))

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Context cancelled, worker stopping", zap.String("name", w.name))
			return
		case <-w.stopChan:
			w.logger.Info("Stop signal received, worker stopping", zap.String("name", w.name))
			return
		case <-ticker.C:
			// Before starting work, do a non-blocking check for the stop signal.
			// This prevents a race where Stop() is called right as we are about to start work.
			select {
			case <-w.stopChan:
				return
			default:
			}
			w.executeWorkFunc(ctx)
		}
	}
}

// executeWorkFunc runs the worker's function, ensuring that Stop() will wait for it to complete.
func (w *BaseWorker) executeWorkFunc(ctx context.Context) {
	w.wg.Add(1)
	defer w.wg.Done()

	// Another check to see if the context was cancelled while we were waiting for the tick.
	select {
	case <-ctx.Done():
		return
	default:
	}

	if err := w.workFunc(ctx); err != nil {
		w.logger.Error("Worker function failed", zap.String("name", w.name), zap.Error(err))
	}
}

// Stop gracefully shuts down the worker.
// It stops the ticker and waits for any in-progress work to complete.
// It is safe to call Stop multiple times.
func (w *BaseWorker) Stop() {
	w.stopOnce.Do(func() {
		w.mu.RLock()
		defer w.mu.RUnlock()
		if !w.started {
			return // Don't try to stop a worker that hasn't been started
		}

		// Close stopChan to signal the Start loop to exit.
		close(w.stopChan)

		// Wait for the last execution of workFunc to complete.
		w.wg.Wait()
	})
}

// Name returns the name of the worker.
func (w *BaseWorker) Name() string {
	return w.name
}
