package core

import (
	"context"
)

type OutboxMessageRepository interface {
	SaveMessage(ctx context.Context, message OutboxMessage) error
	FetchPendingMessages(ctx context.Context, limit int) ([]OutboxMessage, error)
	MarkMessageAsSent(ctx context.Context, id string) error
	MarkMessageAsFailed(ctx context.Context, id string, error string) error
}
