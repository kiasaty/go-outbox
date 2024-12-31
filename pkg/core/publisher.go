package core

import (
	"context"
)

type OutboxMessagePublisher interface {
	Publish(ctx context.Context, message OutboxMessage) error
}
