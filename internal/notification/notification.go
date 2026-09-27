// Package notification delivers status transition alerts.
package notification

import (
	"context"

	"github.com/Olzerq/Pulse/internal/monitorstate"
)

type DeliveryStatus string

const (
	DeliveryPending DeliveryStatus = "pending"
	DeliverySent    DeliveryStatus = "sent"
	DeliverySkipped DeliveryStatus = "skipped"
)

type Delivery struct {
	Transition monitorstate.Transition
	Status     DeliveryStatus
}

type Sender interface {
	Send(context.Context, monitorstate.Transition) (int64, error)
}
