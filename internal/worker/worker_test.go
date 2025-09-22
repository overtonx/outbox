package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestNewBaseWorker(t *testing.T) {
	name := "test-worker"
	interval := 100 * time.Millisecond
	logger := zap.NewNop()
	workFunc := func(ctx context.Context) error { return nil }

	worker := NewBaseWorker(name, interval, logger, workFunc)

	if worker == nil {
		t.Fatal("Expected non-nil worker")
	}

	if worker.name != name {
		t.Errorf("Expected name %s, got %s", name, worker.name)
	}

	if worker.interval != interval {
		t.Errorf("Expected interval %v, got %v", interval, worker.interval)
	}

	if worker.logger != logger {
		t.Error("Expected logger to match")
	}

	if worker.workFunc == nil {
		t.Fatal("Expected non-nil workFunc")
	}

	if worker.stopChan == nil {
		t.Fatal("Expected non-nil stopChan")
	}

	if worker.doneChan == nil {
		t.Fatal("Expected non-nil doneChan")
	}
}

func TestBaseWorkerName(t *testing.T) {
	name := "test-worker"
	worker := NewBaseWorker(name, 100*time.Millisecond, zap.NewNop(), func(ctx context.Context) error { return nil })

	if worker.Name() != name {
		t.Errorf("Expected name %s, got %s", name, worker.Name())
	}
}

func TestBaseWorkerStartAndStop(t *testing.T) {
	worker := NewBaseWorker("test-worker", 50*time.Millisecond, zap.NewNop(), func(ctx context.Context) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	worker.Stop()
	wg.Wait()
}

func TestBaseWorkerWorkFunctionCalled(t *testing.T) {
	callCount := 0
	workFunc := func(ctx context.Context) error {
		callCount++
		return nil
	}

	worker := NewBaseWorker("test-worker", 50*time.Millisecond, zap.NewNop(), workFunc)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx)
	}()

	time.Sleep(100 * time.Millisecond)
	worker.Stop()
	wg.Wait()

	if callCount == 0 {
		t.Error("Expected workFunc to be called at least once")
	}
}

func TestBaseWorkerWorkFunctionError(t *testing.T) {
	workFunc := func(ctx context.Context) error {
		return errors.New("work function error")
	}

	worker := NewBaseWorker("test-worker", 50*time.Millisecond, zap.NewNop(), workFunc)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx)
	}()

	time.Sleep(75 * time.Millisecond)
	worker.Stop()
	wg.Wait()
}

func TestBaseWorkerContextCancellation(t *testing.T) {
	worker := NewBaseWorker("test-worker", 50*time.Millisecond, zap.NewNop(), func(ctx context.Context) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx)
	}()

	wg.Wait()
}

func TestBaseWorkerStopBeforeStart(t *testing.T) {
	worker := NewBaseWorker("test-worker", 50*time.Millisecond, zap.NewNop(), func(ctx context.Context) error { return nil })

	worker.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		worker.Start(ctx)
	}()

	wg.Wait()
}

func TestBaseWorkerMultipleStops(t *testing.T) {
	worker := NewBaseWorker("test-worker", 50*time.Millisecond, zap.NewNop(), func(ctx context.Context) error { return nil })

	worker.Stop()
	worker.Stop()
	worker.Stop()
}

func TestBaseWorkerConcurrentAccess(t *testing.T) {
	worker := NewBaseWorker("test-worker", 50*time.Millisecond, zap.NewNop(), func(ctx context.Context) error { return nil })

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		worker.Start(ctx)
	}()

	go func() {
		defer wg.Done()
		time.Sleep(50 * time.Millisecond)
		worker.Stop()
	}()

	go func() {
		defer wg.Done()
		time.Sleep(100 * time.Millisecond)
		worker.Name()
	}()

	wg.Wait()
}
