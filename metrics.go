package outbox

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MetricsCollector — точка расширения для отправки метрик Dispatcher'а во
// внешнюю систему наблюдаемости. NoOpMetricsCollector используется по
// умолчанию; otelMetrics — рабочая реализация поверх OTel Metrics API
// (сама по себе вендоронезависимая — вывод в Prometheus/иную систему
// настраивается через MeterProvider хост-приложения, а не в этом пакете).
type MetricsCollector interface {
	IncrementCounter(name string, tags map[string]string)
	RecordDuration(name string, duration time.Duration, tags map[string]string)
	RecordGauge(name string, value float64, tags map[string]string)
}

type NoOpMetricsCollector struct{}

func NewNoOpMetricsCollector() *NoOpMetricsCollector {
	return &NoOpMetricsCollector{}
}

func (m *NoOpMetricsCollector) IncrementCounter(name string, tags map[string]string) {}

func (m *NoOpMetricsCollector) RecordDuration(name string, duration time.Duration, tags map[string]string) {
}

func (m *NoOpMetricsCollector) RecordGauge(name string, value float64, tags map[string]string) {}

type otelMetrics struct {
	meter metric.Meter
	mu    sync.RWMutex

	counters   map[string]metric.Int64Counter
	histograms map[string]metric.Float64Histogram
	gauges     map[string]metric.Float64UpDownCounter
}

// NewOTelMetrics создаёт MetricsCollector поверх глобального OTel MeterProvider.
func NewOTelMetrics() *otelMetrics {
	return NewOTelMetricsWithMeter(otel.Meter("outbox"))
}

// NewOTelMetricsWithMeter создаёт MetricsCollector поверх переданного metric.Meter.
func NewOTelMetricsWithMeter(meter metric.Meter) *otelMetrics {
	return &otelMetrics{
		meter:      meter,
		counters:   make(map[string]metric.Int64Counter),
		histograms: make(map[string]metric.Float64Histogram),
		gauges:     make(map[string]metric.Float64UpDownCounter),
	}
}

func (m *otelMetrics) IncrementCounter(name string, tags map[string]string) {
	counter, err := m.getOrCreateCounter(name)
	if err != nil {
		return
	}
	counter.Add(context.Background(), 1, metric.WithAttributes(tagsToAttributes(tags)...))
}

func (m *otelMetrics) RecordDuration(name string, duration time.Duration, tags map[string]string) {
	histogram, err := m.getOrCreateHistogram(name)
	if err != nil {
		return
	}
	histogram.Record(context.Background(), duration.Seconds(), metric.WithAttributes(tagsToAttributes(tags)...))
}

func (m *otelMetrics) RecordGauge(name string, value float64, tags map[string]string) {
	gauge, err := m.getOrCreateGauge(name)
	if err != nil {
		return
	}
	gauge.Add(context.Background(), value, metric.WithAttributes(tagsToAttributes(tags)...))
}

func (m *otelMetrics) getOrCreateCounter(name string) (metric.Int64Counter, error) {
	m.mu.RLock()
	if counter, exists := m.counters[name]; exists {
		m.mu.RUnlock()
		return counter, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if counter, exists := m.counters[name]; exists {
		return counter, nil
	}

	counter, err := m.meter.Int64Counter(name, metric.WithDescription("Counter for "+name), metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	m.counters[name] = counter
	return counter, nil
}

func (m *otelMetrics) getOrCreateHistogram(name string) (metric.Float64Histogram, error) {
	m.mu.RLock()
	if histogram, exists := m.histograms[name]; exists {
		m.mu.RUnlock()
		return histogram, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if histogram, exists := m.histograms[name]; exists {
		return histogram, nil
	}

	histogram, err := m.meter.Float64Histogram(name, metric.WithDescription("Histogram for "+name), metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	m.histograms[name] = histogram
	return histogram, nil
}

func (m *otelMetrics) getOrCreateGauge(name string) (metric.Float64UpDownCounter, error) {
	m.mu.RLock()
	if gauge, exists := m.gauges[name]; exists {
		m.mu.RUnlock()
		return gauge, nil
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()
	if gauge, exists := m.gauges[name]; exists {
		return gauge, nil
	}

	gauge, err := m.meter.Float64UpDownCounter(name, metric.WithDescription("Gauge for "+name), metric.WithUnit("1"))
	if err != nil {
		return nil, err
	}
	m.gauges[name] = gauge
	return gauge, nil
}

func tagsToAttributes(tags map[string]string) []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, len(tags))
	for key, value := range tags {
		attrs = append(attrs, attribute.String(key, value))
	}
	return attrs
}
