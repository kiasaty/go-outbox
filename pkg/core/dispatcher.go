package core

import (
	"context"
)

type OutboxMessageDispatcher interface {
	Dispatch(ctx context.Context) error
}
