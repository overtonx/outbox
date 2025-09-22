package worker

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

type BaseWorker struct {
	name     string
	interval time.Duration
	logger   *zap.Logger
	mu       sync.RWMutex
	started  bool
	stopChan chan struct{}
	workFunc func(ctx context.Context) error
	doneChan chan struct{}
}

func NewBaseWorker(name string, interval time.Duration, logger *zap.Logger, workFunc func(ctx context.Context) error) *BaseWorker {
	return &BaseWorker{
		name:     name,
		interval: interval,
		logger:   logger,
		stopChan: make(chan struct{}),
		doneChan: make(chan struct{}),
		workFunc: workFunc,
	}
}

func (w *BaseWorker) Start(ctx context.Context) {
	w.logger.Info("Starting worker", zap.String("name", w.name), zap.Duration("interval", w.interval))

	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		w.logger.Warn("Worker already started", zap.String("name", w.Name()))
		return
	}
	w.started = true
	w.mu.Unlock()

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	defer close(w.doneChan)

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Context cancelled, stopping worker", zap.String("name", w.name))
			return
		case <-w.stopChan:
			w.logger.Info("Stop signal received, stopping worker", zap.String("name", w.name))
			return
		case <-ticker.C:
			if err := w.workFunc(ctx); err != nil {
				w.logger.Error("Worker execution failed",
					zap.String("name", w.name),
					zap.Error(err))
			}
		}
	}
}

// Stop останавливает воркер и ждет завершения
func (w *BaseWorker) Stop() {
	w.mu.RLock()
	if !w.started {
		w.logger.Info("Worker not started", zap.String("name", w.name))
		w.mu.RUnlock()
		return
	}
	w.mu.RUnlock()

	w.logger.Info("Stopping worker", zap.String("name", w.name))
	close(w.stopChan)

	// Ждем завершения воркера
	<-w.doneChan
	w.logger.Info("Worker stopped", zap.String("name", w.name))
}

// Name возвращает имя воркера
func (w *BaseWorker) Name() string {
	return w.name
}
