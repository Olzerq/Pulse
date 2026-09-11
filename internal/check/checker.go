package check

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/Olzerq/Pulse/internal/monitor"
)

const maxDrainBytes = 64 << 10

// Checker executes HTTP requests with a shared client and connection pool.
type Checker struct {
	client    *http.Client
	transport *http.Transport
	userAgent string
}

func NewChecker(maxConcurrency int, userAgent string) *Checker {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = maxConcurrency * 2
	transport.MaxIdleConnsPerHost = maxConcurrency
	transport.MaxConnsPerHost = maxConcurrency

	return &Checker{
		client:    &http.Client{Transport: transport},
		transport: transport,
		userAgent: userAgent,
	}
}

// CloseIdleConnections releases connections retained for reuse.
func (c *Checker) CloseIdleConnections() {
	c.transport.CloseIdleConnections()
}

// Check performs one request. The per-monitor timeout covers connection setup,
// redirects, and waiting for response headers.
func (c *Checker) Check(ctx context.Context, item monitor.Monitor) Result {
	requestCtx, cancel := context.WithTimeout(ctx, time.Duration(item.TimeoutMS)*time.Millisecond)
	defer cancel()

	startedAt := time.Now()
	request, err := http.NewRequestWithContext(requestCtx, item.Method, item.URL, nil)
	if err != nil {
		return failedResult(item.ID, startedAt, FailureRequest, err)
	}
	request.Header.Set("User-Agent", c.userAgent)

	response, err := c.client.Do(request)
	latency := time.Since(startedAt)
	if err != nil {
		kind := classifyRequestError(ctx, requestCtx, err)
		return Result{
			MonitorID: item.ID,
			CheckedAt: time.Now().UTC(),
			LatencyMS: latency.Milliseconds(),
			ErrorKind: kind,
			Error:     err.Error(),
		}
	}
	defer response.Body.Close()

	// Draining small response bodies allows the transport to reuse connections
	// without allowing an unexpectedly large body to consume unbounded work.
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, maxDrainBytes))

	result := Result{
		MonitorID:  item.ID,
		CheckedAt:  time.Now().UTC(),
		StatusCode: response.StatusCode,
		LatencyMS:  latency.Milliseconds(),
		Success:    response.StatusCode == item.ExpectedStatusCode,
	}
	if !result.Success {
		result.ErrorKind = FailureUnexpectedStatus
		result.Error = fmt.Sprintf("expected HTTP status %d, got %d", item.ExpectedStatusCode, response.StatusCode)
	}
	return result
}

func failedResult(monitorID string, startedAt time.Time, kind FailureKind, err error) Result {
	return Result{
		MonitorID: monitorID,
		CheckedAt: time.Now().UTC(),
		LatencyMS: time.Since(startedAt).Milliseconds(),
		ErrorKind: kind,
		Error:     err.Error(),
	}
}

func classifyRequestError(parentCtx, requestCtx context.Context, err error) FailureKind {
	if errors.Is(parentCtx.Err(), context.Canceled) {
		return FailureCanceled
	}
	if errors.Is(requestCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return FailureTimeout
	}

	var networkError net.Error
	if errors.As(err, &networkError) {
		if networkError.Timeout() {
			return FailureTimeout
		}
		return FailureNetwork
	}

	return FailureNetwork
}
