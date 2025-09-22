package backoff

import (
	"math"
	"time"
)

const (
	defaultBaseDelay = 1 * time.Minute
	defaultMaxDelay  = 30 * time.Minute
)

type BackoffStrategy interface {
	CalculateNextAttempt(attemptCount int) time.Time
}

type LinearBackoffStrategy struct {
	BaseDelay time.Duration
}

func (l *LinearBackoffStrategy) CalculateNextAttempt(attemptCount int) time.Time {
	delay := l.BaseDelay * time.Duration(attemptCount)
	return time.Now().Add(delay)
}

type ExponentialBackoffStrategy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
}

func (e *ExponentialBackoffStrategy) CalculateNextAttempt(attemptCount int) time.Time {
	delay := e.BaseDelay * time.Duration(math.Pow(2, float64(attemptCount-1)))
	if delay > e.MaxDelay {
		delay = e.MaxDelay
	}
	return time.Now().Add(delay)
}

type FixedBackoffStrategy struct {
	Delay time.Duration
}

func (f *FixedBackoffStrategy) CalculateNextAttempt(attemptCount int) time.Time {
	return time.Now().Add(f.Delay)
}

func DefaultBackoffStrategy() BackoffStrategy {
	return &ExponentialBackoffStrategy{
		BaseDelay: defaultBaseDelay,
		MaxDelay:  defaultMaxDelay,
	}
}

func NewLinearBackoffStrategy(baseDelay time.Duration) BackoffStrategy {
	return &LinearBackoffStrategy{BaseDelay: baseDelay}
}

func NewExponentialBackoffStrategy(baseDelay, maxDelay time.Duration) BackoffStrategy {
	return &ExponentialBackoffStrategy{
		BaseDelay: baseDelay,
		MaxDelay:  maxDelay,
	}
}

func NewFixedBackoffStrategy(delay time.Duration) BackoffStrategy {
	return &FixedBackoffStrategy{Delay: delay}
}
