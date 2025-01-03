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

func (r *PostgresRepository) FetchPendingMessages(ctx context.Context, limit uint32, processingLockTimeout uint32) ([]core.OutboxMessage, error) {
	query := `
		WITH selected_messages AS (
			SELECT *
			FROM outbox
			WHERE status = '$1' AND available_at <= NOW()
			ORDER BY available_at ASC
			LIMIT $2
			FOR UPDATE SKIP LOCKED

			UNION ALL

			SELECT *
			FROM outbox
			WHERE status = '$3' AND available_at <= NOW() AND picked_at < NOW() - INTERVAL '$4 seconds'
			ORDER BY available_at ASC
			LIMIT $5
			FOR UPDATE SKIP LOCKED
		)
		UPDATE outbox
		SET status = '$6', picked_at = NOW()
		WHERE id IN (
			SELECT id
			FROM selected_messages
			ORDER BY available_at ASC
			LIMIT $7
		)
		RETURNING id, payload, status, attempts;
	`

	rows, err := r.db.QueryContext(
		ctx,
		query,
		core.MessageStatusPending,
		limit,
		core.MessageStatusProcessing,
		processingLockTimeout,
		limit,
		core.MessageStatusProcessing,
		limit,
	)

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
	return r.updateMessageStatus(ctx, id, core.MessageStatusSent, shouldIncrementAttempts)
}

func (r *PostgresRepository) MarkMessageAsFailed(ctx context.Context, id string, shouldIncrementAttempts bool) error {
	return r.updateMessageStatus(ctx, id, core.MessageStatusFailed, shouldIncrementAttempts)
}

func (r *PostgresRepository) MarkMessageForRetry(ctx context.Context, id string, delay time.Duration, shouldIncrementAttempts bool) error {
	query := "UPDATE outbox SET status = '$1'"

	if shouldIncrementAttempts {
		query += ", attempts = attempts + 1"
	}

	query += ", available_at = (NOW() + ($2 || ' seconds')::INTERVAL)"

	query += " WHERE id = $3;"

	_, err := r.db.ExecContext(ctx, query, core.MessageStatusPending, delay.Seconds(), id)

	return err
}

func (r *PostgresRepository) updateMessageStatus(ctx context.Context, id string, status core.MessageStatus, shouldIncrementAttempts bool) error {
	query := "UPDATE outbox SET status = '$1'"

	if shouldIncrementAttempts {
		query += ", attempts = attempts + 1"
	}

	query += " WHERE id = $2;"

	_, err := r.db.ExecContext(ctx, query, status, id)

	return err
}
