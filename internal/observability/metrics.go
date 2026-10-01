// Package observability contains Prometheus metrics and health endpoints shared
// by Pulse services.
package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns a private registry. Private registries keep service tests
// isolated and prevent duplicate registration when several services run in one
// test process.
type Metrics struct {
	registry      *prometheus.Registry
	checks        *prometheus.CounterVec
	checkDuration *prometheus.HistogramVec
	checkErrors   *prometheus.CounterVec
	monitors      prometheus.Gauge
	monitorsDown  prometheus.Gauge
	kafkaEvents   *prometheus.CounterVec
	notifications *prometheus.CounterVec
	lockAttempts  *prometheus.CounterVec
	httpRequests  *prometheus.CounterVec
	httpDuration  *prometheus.HistogramVec
}

func NewMetrics(service string) *Metrics {
	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	registerer := prometheus.WrapRegistererWith(prometheus.Labels{"service": service}, registry)

	metrics := &Metrics{
		registry: registry,
		checks: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pulse",
			Name:      "checks_total",
			Help:      "Number of HTTP checks completed by Pinger.",
		}, []string{"result"}),
		checkDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pulse",
			Name:      "check_duration_seconds",
			Help:      "HTTP check latency in seconds.",
			Buckets:   []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"result"}),
		checkErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pulse",
			Name:      "check_errors_total",
			Help:      "Number of failed HTTP checks by bounded error kind.",
		}, []string{"kind"}),
		monitors: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pulse",
			Name:      "monitors_total",
			Help:      "Number of active monitors known to Pinger.",
		}),
		monitorsDown: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "pulse",
			Name:      "monitors_down",
			Help:      "Number of active monitors whose current Redis state is DOWN.",
		}),
		kafkaEvents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pulse",
			Name:      "kafka_events_total",
			Help:      "Number of Kafka events handled by direction and result.",
		}, []string{"direction", "result"}),
		notifications: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pulse",
			Name:      "notifications_total",
			Help:      "Number of notifications handled by channel and result.",
		}, []string{"channel", "result"}),
		lockAttempts: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pulse",
			Name:      "lock_attempts_total",
			Help:      "Number of distributed lock attempts by result.",
		}, []string{"result"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "pulse",
			Name:      "http_requests_total",
			Help:      "Number of HTTP requests by method, route, and status.",
		}, []string{"method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "pulse",
			Name:      "http_request_duration_seconds",
			Help:      "HTTP request duration in seconds by method and route.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method", "route"}),
	}

	registerer.MustRegister(
		metrics.checks,
		metrics.checkDuration,
		metrics.checkErrors,
		metrics.monitors,
		metrics.monitorsDown,
		metrics.kafkaEvents,
		metrics.notifications,
		metrics.lockAttempts,
		metrics.httpRequests,
		metrics.httpDuration,
	)
	return metrics
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{EnableOpenMetrics: true})
}

func (m *Metrics) Registry() *prometheus.Registry {
	return m.registry
}

func (m *Metrics) ObserveCheck(success bool, errorKind string, duration time.Duration) {
	result := "failure"
	if success {
		result = "success"
	}
	m.checks.WithLabelValues(result).Inc()
	m.checkDuration.WithLabelValues(result).Observe(duration.Seconds())
	if !success {
		if errorKind == "" {
			errorKind = "unknown"
		}
		m.checkErrors.WithLabelValues(errorKind).Inc()
	}
}

func (m *Metrics) SetMonitorCounts(total, down int) {
	m.SetMonitorsTotal(total)
	m.SetMonitorsDown(down)
}

func (m *Metrics) SetMonitorsTotal(total int) {
	m.monitors.Set(float64(total))
}

func (m *Metrics) SetMonitorsDown(down int) {
	m.monitorsDown.Set(float64(down))
}

func (m *Metrics) ObserveKafka(direction, result string) {
	m.kafkaEvents.WithLabelValues(direction, result).Inc()
}

func (m *Metrics) ObserveNotification(channel, result string) {
	m.notifications.WithLabelValues(channel, result).Inc()
}

func (m *Metrics) ObserveLock(result string) {
	m.lockAttempts.WithLabelValues(result).Inc()
}

func (m *Metrics) ObserveHTTP(method, route string, status int, duration time.Duration) {
	m.httpRequests.WithLabelValues(method, route, strconv.Itoa(status)).Inc()
	m.httpDuration.WithLabelValues(method, route).Observe(duration.Seconds())
}
