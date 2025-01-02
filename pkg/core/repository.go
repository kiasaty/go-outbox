package core

import (
	"context"
	"time"
)

type OutboxMessageRepository interface {
	SaveMessage(ctx context.Context, message OutboxMessage) error
	FetchPendingMessages(ctx context.Context, limit int) ([]OutboxMessage, error)
	MarkMessageAsSent(ctx context.Context, id string, shouldIncrementAttempts bool) error
	MarkMessageAsFailed(ctx context.Context, id string, shouldIncrementAttempts bool) error
	MarkMessageForRetry(ctx context.Context, id string, delay time.Duration, shouldIncrementAttempts bool) error
}
