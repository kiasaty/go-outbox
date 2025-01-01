package postgresql

import (
	"context"
	"database/sql"
	"go-transactional-outbox/pkg/core"
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
		"INSERT INTO outbox (id, payload, status) VALUES ($1, $2, $3)",
		message.ID, message.Payload, message.Status)
	return err
}

func (r *PostgresRepository) FetchPendingMessages(ctx context.Context, limit int) ([]core.OutboxMessage, error) {
	rows, err := r.db.QueryContext(ctx,
		"SELECT id, payload, status FROM outbox WHERE status='pending' LIMIT $1", limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var messages []core.OutboxMessage
	for rows.Next() {
		var message core.OutboxMessage
		if err := rows.Scan(&message.ID, &message.Payload, &message.Status); err != nil {
			return nil, err
		}
		messages = append(messages, message)
	}

	return messages, nil
}

func (r *PostgresRepository) MarkMessageAsSent(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE outbox SET status='sent' WHERE id=$1", id)
	return err
}

func (r *PostgresRepository) MarkMessageAsFailed(ctx context.Context, id string, error string) error {
	_, err := r.db.ExecContext(ctx,
		"UPDATE outbox SET status='failed' WHERE id=$1", id)
	return err
}
