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
