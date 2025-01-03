package postgresql

import (
	"context"
	"database/sql"
	"go-transactional-outbox/pkg/core"
	"log"
	"os"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDB *sql.DB

func TestMain(m *testing.M) {
	setupDatabase()

	// Run tests
	code := m.Run()

	teardownDatabase()

	os.Exit(code)
}

func setupDatabase() {
	var err error

	// Connect to PostgreSQL test database
	testDB, err = sql.Open("postgres", "host=localhost port=5432 user=postgres password=secret dbname=testdb sslmode=disable")

	if err != nil {
		log.Fatalf("Failed to connect to test database: %v", err)
	}

	// Create outbox table
	_, err = testDB.Exec(`
		CREATE TABLE IF NOT EXISTS outbox (
			id SERIAL PRIMARY KEY,
			payload TEXT NOT NULL,
			status VARCHAR(50) NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		)
	`)

	if err != nil {
		log.Fatalf("Failed to create test table: %v", err)
	}
}

func teardownDatabase() {
	_, _ = testDB.Exec(`DROP TABLE IF EXISTS outbox`)

	testDB.Close()
}

func setupTest(t *testing.T) (*sql.Tx, context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel) // Ensure the timeout context is canceled after the test

	// Begin a database transaction before each test.
	tx, err := testDB.BeginTx(ctx, &sql.TxOptions{
		Isolation: sql.LevelSerializable,
	})
	require.NoError(t, err, "Failed to start transaction")

	// Rollback the database transaction after the test.
	t.Cleanup(func() {
		if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
			t.Errorf("Failed to rollback transaction: %v", err)
		}
	})

	return tx, ctx
}

func TestSaveMessage(t *testing.T) {
	tx, ctx := setupTest(t)

	repo := PostgresRepository{
		db: tx,
	}

	message := core.OutboxMessage{
		ID:      "1",
		Payload: "Test Payload",
		Status:  core.MessageStatusPending,
	}

	err := repo.SaveMessage(ctx, message)
	require.NoError(t, err, "Failed to save message")

	var count int
	err = tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM outbox WHERE id = $1`, message.ID).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "Message was not saved correctly")
}

func TestFetchPendingMessages(t *testing.T) {
	tx, ctx := setupTest(t)

	repo := PostgresRepository{
		db: tx,
	}

	// Insert sample pending messages
	_, err := tx.ExecContext(ctx, `
		INSERT INTO outbox (id, payload, status) 
		VALUES ($1, $2, $3), ($4, $5, $6)`,
		"2", "Payload 1", core.MessageStatusPending,
		"3", "Payload 2", core.MessageStatusPending,
	)
	require.NoError(t, err)

	messages, err := repo.FetchPendingMessages(ctx, 10, 30)
	require.NoError(t, err)
	assert.Len(t, messages, 2, "Expected 2 pending messages")

	assert.Equal(t, "Payload 1", messages[0].Payload)
	assert.Equal(t, "Payload 2", messages[1].Payload)
}

func TestMarkMessageAsSent(t *testing.T) {
	tx, ctx := setupTest(t)

	repo := PostgresRepository{
		db: tx,
	}

	// Insert a pending message
	_, err := tx.ExecContext(ctx, `
		INSERT INTO outbox (id, payload, status) 
		VALUES ($1, $2, $3)`,
		"4", "Payload Sent", core.MessageStatusPending,
	)
	require.NoError(t, err)

	// Mark message as sent
	err = repo.MarkMessageAsSent(ctx, "4")
	require.NoError(t, err)

	// Verify in database
	var status string
	err = tx.QueryRowContext(ctx, `SELECT status FROM outbox WHERE id = $1`, "4").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, core.MessageStatusSent, status, "Message status was not updated to sent")
}

func TestMarkMessageAsFailed(t *testing.T) {
	tx, ctx := setupTest(t)

	repo := PostgresRepository{
		db: tx,
	}

	// Insert a pending message
	_, err := tx.ExecContext(ctx, `
		INSERT INTO outbox (id, payload, status) 
		VALUES ($1, $2, $3)`,
		"5", "Payload Failed", core.MessageStatusPending,
	)
	require.NoError(t, err)

	// Mark message as failed
	err = repo.MarkMessageAsFailed(ctx, "5", "Error: Timeout")
	require.NoError(t, err)

	// Verify in database
	var status string
	err = tx.QueryRowContext(ctx, `SELECT status FROM outbox WHERE id = $1`, "5").Scan(&status)
	require.NoError(t, err)
	assert.Equal(t, core.MessageStatusFailed, status, "Message status was not updated to failed")
}
