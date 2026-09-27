package notification

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Olzerq/Pulse/internal/check"
	"github.com/Olzerq/Pulse/internal/monitorstate"
)

func TestTelegramSender(t *testing.T) {
	t.Parallel()

	var received struct {
		ChatID string `json:"chat_id"`
		Text   string `json:"text"`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/bottest-token/sendMessage" {
			t.Errorf("request path = %q", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Errorf("decode request: %v", err)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":42}}`))
	}))
	defer server.Close()

	sender := NewTelegramSender("test-token", "12345", server.URL, time.Second)
	messageID, err := sender.Send(context.Background(), downTransition())
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if messageID != 42 {
		t.Errorf("message ID = %d, want 42", messageID)
	}
	if received.ChatID != "12345" {
		t.Errorf("chat ID = %q, want 12345", received.ChatID)
	}
	if !strings.Contains(received.Text, "UP -> DOWN") || !strings.Contains(received.Text, "expected 200") {
		t.Errorf("message text = %q", received.Text)
	}
}

func TestTelegramSenderRejectsAPIError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"ok":false,"error_code":400,"description":"chat not found"}`))
	}))
	defer server.Close()

	sender := NewTelegramSender("test-token", "12345", server.URL, time.Second)
	if _, err := sender.Send(context.Background(), downTransition()); err == nil {
		t.Fatal("Send() error = nil, want Telegram API error")
	}
}

func downTransition() monitorstate.Transition {
	statusCode := http.StatusServiceUnavailable
	reason := "expected 200, got 503"
	return monitorstate.Transition{
		EventID:       "2efad0fa-47c9-4ff5-b2c8-a61735ac1251",
		MonitorID:     "9606cfdf-8eaf-4e9c-bd17-16e3e2b63748",
		Previous:      monitorstate.StatusUp,
		Current:       monitorstate.StatusDown,
		ChangedAt:     time.Now().UTC(),
		StatusCode:    &statusCode,
		LatencyMS:     42,
		FailureKind:   check.FailureUnexpectedStatus,
		FailureReason: &reason,
	}
}
