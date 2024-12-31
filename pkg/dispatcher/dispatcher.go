package dispatcher

import (
	"context"
	"fmt"
	"go-transactional-outbox/pkg/core"
)

type DefaultOutboxMessageDispatcher struct {
	repository core.OutboxMessageRepository
	publisher  core.OutboxMessagePublisher
}

func (d *DefaultOutboxMessageDispatcher) Dispatch(ctx context.Context) error {
	messages, err := d.repository.FetchPendingMessages(ctx, 10)
	if err != nil {
		return fmt.Errorf("failed to fetch messages: %w", err)
	}

	for _, message := range messages {
		if err := d.publisher.Publish(ctx, message); err != nil {
			_ = d.repository.MarkMessageAsFailed(ctx, message.ID, err.Error())
			continue
		}

		_ = d.repository.MarkMessageAsSent(ctx, message.ID)
	}

	return nil
}
