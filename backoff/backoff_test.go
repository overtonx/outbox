package backoff

import (
	"testing"
	"time"
)

func TestLinearBackoffStrategy(t *testing.T) {
	baseDelay := 120 * time.Millisecond
	strategy := &LinearBackoffStrategy{BaseDelay: baseDelay}

	tests := []struct {
		name         string
		attemptCount int
		expectedMin  time.Duration
		expectedMax  time.Duration
	}{
		{"first attempt", 1, 100 * time.Millisecond, 200 * time.Millisecond},
		{"second attempt", 2, 200 * time.Millisecond, 300 * time.Millisecond},
		{"third attempt", 3, 300 * time.Millisecond, 400 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nextAttempt := strategy.CalculateNextAttempt(tt.attemptCount)
			now := time.Now()
			delay := nextAttempt.Sub(now)

			if delay < tt.expectedMin || delay > tt.expectedMax {
				t.Errorf("Expected delay between %v and %v, got %v", tt.expectedMin, tt.expectedMax, delay)
			}
		})
	}
}

func TestExponentialBackoffStrategy(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 1 * time.Second
	strategy := &ExponentialBackoffStrategy{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
	}

	tests := []struct {
		name         string
		attemptCount int
		expectedMin  time.Duration
		expectedMax  time.Duration
	}{
		{"first attempt", 1, 90 * time.Millisecond, 200 * time.Millisecond},
		{"second attempt", 2, 190 * time.Millisecond, 300 * time.Millisecond},
		{"third attempt", 3, 390 * time.Millisecond, 500 * time.Millisecond},
		{"exceeds max delay", 10, 900 * time.Millisecond, 1100 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nextAttempt := strategy.CalculateNextAttempt(tt.attemptCount)
			now := time.Now()
			delay := nextAttempt.Sub(now)

			if delay < tt.expectedMin || delay > tt.expectedMax {
				t.Errorf("Expected delay between %v and %v, got %v", tt.expectedMin, tt.expectedMax, delay)
			}
		})
	}
}

func TestFixedBackoffStrategy(t *testing.T) {
	delay := 500 * time.Millisecond
	strategy := &FixedBackoffStrategy{Delay: delay}

	tests := []struct {
		name         string
		attemptCount int
		expectedMin  time.Duration
		expectedMax  time.Duration
	}{
		{"first attempt", 1, 490 * time.Millisecond, 600 * time.Millisecond},
		{"second attempt", 2, 490 * time.Millisecond, 600 * time.Millisecond},
		{"third attempt", 3, 490 * time.Millisecond, 600 * time.Millisecond},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			nextAttempt := strategy.CalculateNextAttempt(tt.attemptCount)
			now := time.Now()
			delay := nextAttempt.Sub(now)

			if delay < tt.expectedMin || delay > tt.expectedMax {
				t.Errorf("Expected delay between %v and %v, got %v", tt.expectedMin, tt.expectedMax, delay)
			}
		})
	}
}

func TestDefaultBackoffStrategy(t *testing.T) {
	strategy := DefaultBackoffStrategy()
	if strategy == nil {
		t.Fatal("Expected non-nil strategy")
	}

	nextAttempt := strategy.CalculateNextAttempt(1)
	now := time.Now()
	delay := nextAttempt.Sub(now)

	if delay < 0 {
		t.Errorf("Expected positive delay, got %v", delay)
	}
}

func TestNewLinearBackoffStrategy(t *testing.T) {
	baseDelay := 200 * time.Millisecond
	strategy := NewLinearBackoffStrategy(baseDelay)

	if strategy == nil {
		t.Fatal("Expected non-nil strategy")
	}

	linearStrategy, ok := strategy.(*LinearBackoffStrategy)
	if !ok {
		t.Fatal("Expected LinearBackoffStrategy type")
	}

	if linearStrategy.BaseDelay != baseDelay {
		t.Errorf("Expected BaseDelay %v, got %v", baseDelay, linearStrategy.BaseDelay)
	}
}

func TestNewExponentialBackoffStrategy(t *testing.T) {
	baseDelay := 100 * time.Millisecond
	maxDelay := 2 * time.Second
	strategy := NewExponentialBackoffStrategy(baseDelay, maxDelay)

	if strategy == nil {
		t.Fatal("Expected non-nil strategy")
	}

	expStrategy, ok := strategy.(*ExponentialBackoffStrategy)
	if !ok {
		t.Fatal("Expected ExponentialBackoffStrategy type")
	}

	if expStrategy.BaseDelay != baseDelay {
		t.Errorf("Expected BaseDelay %v, got %v", baseDelay, expStrategy.BaseDelay)
	}

	if expStrategy.MaxDelay != maxDelay {
		t.Errorf("Expected MaxDelay %v, got %v", maxDelay, expStrategy.MaxDelay)
	}
}

func TestNewFixedBackoffStrategy(t *testing.T) {
	delay := 300 * time.Millisecond
	strategy := NewFixedBackoffStrategy(delay)

	if strategy == nil {
		t.Fatal("Expected non-nil strategy")
	}

	fixedStrategy, ok := strategy.(*FixedBackoffStrategy)
	if !ok {
		t.Fatal("Expected FixedBackoffStrategy type")
	}

	if fixedStrategy.Delay != delay {
		t.Errorf("Expected Delay %v, got %v", delay, fixedStrategy.Delay)
	}
}
