package outbox

import (
	"context"
	"sync"

	"go.uber.org/zap"
)

// Dispatcher управляет жизненным циклом воркеров.
type Dispatcher struct {
	logger   *zap.Logger
	workers  []Worker
	mu       sync.RWMutex
	started  bool
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewDispatcher создает новый экземпляр Dispatcher.
// Он принимает логгер и список воркеров для управления.
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

// Start запускает всех воркеров и блокируется до тех пор, пока не будет получен сигнал остановки.
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

	for _, worker := range d.workers {
		d.wg.Add(1)
		go func(w Worker) {
			defer d.wg.Done()
			w.Start(ctx)
		}(worker)
	}

	// Ожидаем сигнала остановки
	select {
	case <-ctx.Done():
		d.logger.Info("Context cancelled, stopping dispatcher")
	case <-d.stopChan:
		d.logger.Info("Stop signal received, stopping dispatcher")
	}

	// Останавливаем всех воркеров
	for _, worker := range d.workers {
		worker.Stop()
	}

	// Ждем завершения всех горутин воркеров
	d.wg.Wait()

	d.mu.Lock()
	d.started = false
	d.mu.Unlock()

	d.logger.Info("Dispatcher stopped")
}

// Stop инициирует остановку Dispatcher.
func (d *Dispatcher) Stop() {
	d.mu.RLock()
	if !d.started {
		d.mu.RUnlock()
		return
	}
	d.mu.RUnlock()

	d.logger.Info("Stopping dispatcher...")
	close(d.stopChan)
}

// IsStarted проверяет, запущен ли Dispatcher.
func (d *Dispatcher) IsStarted() bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.started
}
