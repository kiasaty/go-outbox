package postgresql

import (
	"context"
	"database/sql"
	"go-transactional-outbox/pkg/core"
	"time"
)

// SQLExecutor represents shared methods between *sql.DB and *sql.Tx
type SQLExecutor interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type PostgresRepository struct {
	db SQLExecutor
}

func (r *PostgresRepository) SaveMessage(ctx context.Context, message core.OutboxMessage) error {
	_, err := r.db.ExecContext(ctx,
		"INSERT INTO outbox (id, payload, status, attempts, available_at, created_at) VALUES ($1, $2, $3, 0, NOW(), NOW())",
		message.ID, message.Payload, message.Status)
	return err
}

func (r *PostgresRepository) FetchPendingMessages(ctx context.Context, limit uint32) ([]core.OutboxMessage, error) {
	rows, err := r.db.QueryContext(ctx,
		"SELECT id, payload, status, attempts FROM outbox WHERE status = 'pending' and available_at <= NOW() LIMIT $1", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []core.OutboxMessage
	for rows.Next() {
		var message core.OutboxMessage
		if err := rows.Scan(&message.ID, &message.Payload, &message.Status, &message.Attempts); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}

	return messages, nil
}

func (r *PostgresRepository) MarkMessageAsSent(ctx context.Context, id string, shouldIncrementAttempts bool) error {
	return r.updateMessageStatus(ctx, id, "sent", shouldIncrementAttempts)
}

func (r *PostgresRepository) MarkMessageAsFailed(ctx context.Context, id string, shouldIncrementAttempts bool) error {
	return r.updateMessageStatus(ctx, id, "failed", shouldIncrementAttempts)
}

func (r *PostgresRepository) MarkMessageForRetry(ctx context.Context, id string, delay time.Duration, shouldIncrementAttempts bool) error {
	query := "UPDATE outbox SET status = '$1'"

	if shouldIncrementAttempts {
		query += ", attempts = attempts + 1"
	}

	query += ", available_at = (NOW() + ($2 || ' seconds')::INTERVAL)"

	query += " WHERE id = $3;"

	_, err := r.db.ExecContext(ctx, query, "pending", delay.Seconds(), id)

	return err
}

func (r *PostgresRepository) updateMessageStatus(ctx context.Context, id string, status string, shouldIncrementAttempts bool) error {
	query := "UPDATE outbox SET status = '$1'"

	if shouldIncrementAttempts {
		query += ", attempts = attempts + 1"
	}

	query += " WHERE id = $2;"

	_, err := r.db.ExecContext(ctx, query, status, id)

	return err
}
