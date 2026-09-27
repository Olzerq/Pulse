package notification

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Olzerq/Pulse/internal/monitorstate"
)

const maxTelegramResponseBytes = 1 << 20

type TelegramSender struct {
	client *http.Client
	url    string
	chatID string
	secret string
}

func NewTelegramSender(token, chatID, baseURL string, timeout time.Duration) *TelegramSender {
	return &TelegramSender{
		client: &http.Client{Timeout: timeout},
		url:    strings.TrimRight(baseURL, "/") + "/bot" + token + "/sendMessage",
		chatID: strings.TrimSpace(chatID),
		secret: token,
	}
}

func (s *TelegramSender) Send(ctx context.Context, transition monitorstate.Transition) (int64, error) {
	payload := struct {
		ChatID                string `json:"chat_id"`
		Text                  string `json:"text"`
		DisableWebPagePreview bool   `json:"disable_web_page_preview"`
	}{
		ChatID:                s.chatID,
		Text:                  FormatTelegramMessage(transition),
		DisableWebPagePreview: true,
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("marshal Telegram request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return 0, errors.New("create Telegram request")
	}
	request.Header.Set("Content-Type", "application/json")

	response, err := s.client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("send Telegram request: %s", s.redact(err.Error()))
	}
	defer response.Body.Close()

	var result struct {
		OK          bool   `json:"ok"`
		ErrorCode   int    `json:"error_code"`
		Description string `json:"description"`
		Result      struct {
			MessageID int64 `json:"message_id"`
		} `json:"result"`
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxTelegramResponseBytes))
	if err := decoder.Decode(&result); err != nil {
		return 0, fmt.Errorf("decode Telegram response with HTTP status %d: %w", response.StatusCode, err)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices || !result.OK {
		description := strings.TrimSpace(result.Description)
		if description == "" {
			description = http.StatusText(response.StatusCode)
		}
		return 0, fmt.Errorf(
			"Telegram API rejected request: HTTP %d, code %d, %s",
			response.StatusCode,
			result.ErrorCode,
			s.redact(description),
		)
	}
	if result.Result.MessageID == 0 {
		return 0, errors.New("Telegram response does not contain message_id")
	}

	return result.Result.MessageID, nil
}

func FormatTelegramMessage(transition monitorstate.Transition) string {
	title := "Pulse: сервис восстановлен"
	if transition.Current == monitorstate.StatusDown {
		title = "Pulse: сервис недоступен"
	}

	lines := []string{
		title,
		"Monitor: " + transition.MonitorID,
		"Статус: " + string(transition.Previous) + " -> " + string(transition.Current),
		"Время: " + transition.ChangedAt.UTC().Format(time.RFC3339),
		"Latency: " + strconv.FormatInt(transition.LatencyMS, 10) + " ms",
	}
	if transition.StatusCode != nil {
		lines = append(lines, "HTTP status: "+strconv.Itoa(*transition.StatusCode))
	}
	if transition.FailureReason != nil {
		lines = append(lines, "Причина: "+truncate(*transition.FailureReason, 500))
	}

	return strings.Join(lines, "\n")
}

func (s *TelegramSender) redact(value string) string {
	if s.secret == "" {
		return value
	}
	return strings.ReplaceAll(value, s.secret, "[REDACTED]")
}

func truncate(value string, maxRunes int) string {
	if utf8.RuneCountInString(value) <= maxRunes {
		return value
	}
	runes := []rune(value)
	return string(runes[:maxRunes]) + "..."
}
