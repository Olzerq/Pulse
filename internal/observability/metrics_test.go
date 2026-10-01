package observability

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsExposeBoundedLabels(t *testing.T) {
	t.Parallel()

	metrics := NewMetrics("pinger")
	metrics.ObserveCheck(false, "timeout", 250*time.Millisecond)
	metrics.ObserveKafka("published", "success")
	metrics.ObserveLock("acquired")
	metrics.SetMonitorCounts(4, 1)

	count, err := testutil.GatherAndCount(
		metrics.Registry(),
		"pulse_checks_total",
		"pulse_check_duration_seconds",
		"pulse_check_errors_total",
		"pulse_kafka_events_total",
		"pulse_lock_attempts_total",
		"pulse_monitors_total",
		"pulse_monitors_down",
	)
	if err != nil {
		t.Fatalf("GatherAndCount() error = %v", err)
	}
	if count != 7 {
		t.Fatalf("metric family count = %d, want 7", count)
	}

	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatalf("Gather() error = %v", err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			for _, label := range metric.Label {
				if strings.Contains(label.GetName(), "monitor") || label.GetName() == "url" {
					t.Fatalf("metric %s contains high-cardinality label %s", family.GetName(), label.GetName())
				}
			}
		}
	}
}
