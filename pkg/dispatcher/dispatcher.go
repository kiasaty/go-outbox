package dispatcher

import (
	"context"
	"fmt"
	"go-transactional-outbox/pkg/core"
)

type DefaultOutboxMessageDispatcher struct {
	repository core.OutboxMessageRepository
	publisher  core.OutboxMessagePublisher
	configs    DispatcherConfigs
}

func (d *DefaultOutboxMessageDispatcher) Dispatch(ctx context.Context) error {
	messages, err := d.repository.FetchPendingMessages(ctx, d.configs.FetchLimit, d.configs.ProcessingLockTimeout)
	if err != nil {
		return fmt.Errorf("failed to fetch messages: %w", err)
	}

	for _, message := range messages {
		if message.GetRetryAttempts() >= d.configs.Retry.MaxRetryAttempts {
			_ = d.repository.MarkMessageAsFailed(ctx, message.ID, false)
			continue
		}

		err := d.publisher.Publish(ctx, message)

		currentAttempt := message.Attempts + 1

		if err != nil {
			if message.GetRetryAttempts() < d.configs.Retry.MaxRetryAttempts {
				_ = d.repository.MarkMessageForRetry(
					ctx,
					message.ID,
					d.calculateRetryDelay(currentAttempt),
					true,
				)
			} else {
				_ = d.repository.MarkMessageAsFailed(ctx, message.ID, true)
			}

			continue
		}

		_ = d.repository.MarkMessageAsSent(ctx, message.ID, true)
	}

	return nil
}
