// Package check contains HTTP check execution and result types.
package check

import "time"

// FailureKind classifies a failed check without coupling it to a concrete
// network error string.
type FailureKind string

const (
	FailureNone             FailureKind = ""
	FailureTimeout          FailureKind = "timeout"
	FailureCanceled         FailureKind = "canceled"
	FailureNetwork          FailureKind = "network"
	FailureRequest          FailureKind = "request"
	FailureUnexpectedStatus FailureKind = "unexpected_status"
)

// Result is the outcome of one HTTP check. In Stage 3 it is logged by the
// Pinger. In Stage 4 it becomes the basis of the Kafka check.result event.
type Result struct {
	MonitorID  string
	CheckedAt  time.Time
	Success    bool
	StatusCode int
	LatencyMS  int64
	ErrorKind  FailureKind
	Error      string
}
