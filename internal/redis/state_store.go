package redis

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/Olzerq/Pulse/internal/monitorstate"
)

type keyValueClient interface {
	Set(context.Context, string, string) error
	Get(context.Context, string) (string, error)
	MGet(context.Context, ...string) ([]any, error)
}

type StateStore struct {
	client keyValueClient
}

func NewStateStore(client keyValueClient) *StateStore {
	return &StateStore{client: client}
}

func (s *StateStore) Set(ctx context.Context, state monitorstate.State) error {
	value, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("marshal monitor state: %w", err)
	}
	if err := s.client.Set(ctx, stateKey(state.MonitorID), string(value)); err != nil {
		return fmt.Errorf("store monitor state: %w", err)
	}
	return nil
}

func (s *StateStore) Get(ctx context.Context, monitorID string) (monitorstate.State, error) {
	value, err := s.client.Get(ctx, stateKey(monitorID))
	if errors.Is(err, errKeyNotFound) {
		return monitorstate.Unknown(monitorID), nil
	}
	if err != nil {
		return monitorstate.State{}, fmt.Errorf("load monitor state: %w", err)
	}

	return decodeState(value, monitorID)
}

// CountByStatus counts only the requested active monitors. MGET keeps the
// Prometheus refresh to one Redis round trip and ignores monitors with no state
// because those are UNKNOWN, not DOWN.
func (s *StateStore) CountByStatus(
	ctx context.Context,
	monitorIDs []string,
	status monitorstate.Status,
) (int, error) {
	if len(monitorIDs) == 0 {
		return 0, nil
	}

	keys := make([]string, len(monitorIDs))
	for index, monitorID := range monitorIDs {
		keys[index] = stateKey(monitorID)
	}
	values, err := s.client.MGet(ctx, keys...)
	if err != nil {
		return 0, fmt.Errorf("load monitor states: %w", err)
	}

	count := 0
	for index, raw := range values {
		if raw == nil {
			continue
		}
		value, ok := raw.(string)
		if !ok {
			return 0, fmt.Errorf("decode monitor state %q: Redis value is not a string", monitorIDs[index])
		}
		state, err := decodeState(value, monitorIDs[index])
		if err != nil {
			return 0, err
		}
		if state.Status == status {
			count++
		}
	}
	return count, nil
}

func decodeState(value, monitorID string) (monitorstate.State, error) {
	var state monitorstate.State
	if err := json.Unmarshal([]byte(value), &state); err != nil {
		return monitorstate.State{}, fmt.Errorf("decode monitor state: %w", err)
	}
	// Stage 6 values predate last_status_change_at. Treat their latest check as
	// the best available baseline so rolling upgrades do not poison Consumer.
	if state.LastStatusChangeAt == nil && state.CheckedAt != nil {
		state.LastStatusChangeAt = state.CheckedAt
	}
	if err := state.Validate(monitorID); err != nil {
		return monitorstate.State{}, fmt.Errorf("validate monitor state: %w", err)
	}
	return state, nil
}

func stateKey(monitorID string) string {
	return "monitor:" + monitorID + ":status"
}
