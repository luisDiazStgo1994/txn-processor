package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// PostgresRepository implements Repository using PostgreSQL.
type PostgresRepository struct {
	db *sql.DB
}

// NewPostgresRepository opens a connection to PostgreSQL and verifies it with a ping.
func NewPostgresRepository(dsn string) (*PostgresRepository, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("storage: open db: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("storage: ping db: %w", err)
	}
	return &PostgresRepository{db: db}, nil
}

// --- Account ---

func (r *PostgresRepository) UpsertAccount(ctx context.Context, account Account) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO accounts (account_id, email)
		 VALUES ($1, $2)
		 ON CONFLICT (account_id) DO UPDATE SET email = EXCLUDED.email`,
		account.AccountID, account.Email,
	)
	if err != nil {
		return fmt.Errorf("storage: upsert account: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetAccount(ctx context.Context, accountID string) (Account, error) {
	var a Account
	err := r.db.QueryRowContext(ctx,
		`SELECT account_id, email FROM accounts WHERE account_id = $1`,
		accountID,
	).Scan(&a.AccountID, &a.Email)
	if err != nil {
		return Account{}, fmt.Errorf("storage: get account %q: %w", accountID, err)
	}
	return a, nil
}

// --- FileProcessing ---

func (r *PostgresRepository) CreateFileProcessing(ctx context.Context, fp FileProcessing) error {
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO file_processing
		   (idempotency_key, account_id, status, checkpoint_row, heartbeat_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		fp.IdempotencyKey, fp.AccountID, fp.Status,
		fp.CheckpointRow, fp.HeartbeatAt, time.Now().UTC(),
	)
	if err != nil {
		return fmt.Errorf("storage: create file processing: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetFileProcessing(ctx context.Context, idempotencyKey string) (FileProcessing, error) {
	var fp FileProcessing
	err := r.db.QueryRowContext(ctx,
		`SELECT idempotency_key, account_id, status, checkpoint_row, heartbeat_at, updated_at
		 FROM file_processing WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(&fp.IdempotencyKey, &fp.AccountID, &fp.Status,
		&fp.CheckpointRow, &fp.HeartbeatAt, &fp.UpdatedAt)
	if err != nil {
		return FileProcessing{}, fmt.Errorf("storage: get file processing %q: %w", idempotencyKey, err)
	}
	return fp, nil
}

func (r *PostgresRepository) UpdateFileProcessing(ctx context.Context, fp FileProcessing) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE file_processing
		 SET status = $1, checkpoint_row = $2, heartbeat_at = $3, updated_at = $4
		 WHERE idempotency_key = $5`,
		fp.Status, fp.CheckpointRow, fp.HeartbeatAt, time.Now().UTC(), fp.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("storage: update file processing %q: %w", fp.IdempotencyKey, err)
	}
	return nil
}

// --- FileSummary ---

func (r *PostgresRepository) CreateFileSummary(ctx context.Context, fs FileSummary) error {
	now := time.Now().UTC()
	_, err := r.db.ExecContext(ctx,
		`INSERT INTO file_summary
		   (idempotency_key, account_id, email_sent, summary, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		fs.IdempotencyKey, fs.AccountID, fs.EmailSent, fs.SummaryJSON, now, now,
	)
	if err != nil {
		return fmt.Errorf("storage: create file summary: %w", err)
	}
	return nil
}

func (r *PostgresRepository) GetFileSummary(ctx context.Context, idempotencyKey string) (FileSummary, error) {
	var fs FileSummary
	err := r.db.QueryRowContext(ctx,
		`SELECT idempotency_key, account_id, email_sent, summary, created_at, updated_at
		 FROM file_summary WHERE idempotency_key = $1`,
		idempotencyKey,
	).Scan(&fs.IdempotencyKey, &fs.AccountID, &fs.EmailSent,
		&fs.SummaryJSON, &fs.CreatedAt, &fs.UpdatedAt)
	if err != nil {
		return FileSummary{}, fmt.Errorf("storage: get file summary %q: %w", idempotencyKey, err)
	}
	return fs, nil
}

func (r *PostgresRepository) UpdateFileSummary(ctx context.Context, fs FileSummary) error {
	_, err := r.db.ExecContext(ctx,
		`UPDATE file_summary
		 SET email_sent = $1, summary = $2, updated_at = $3
		 WHERE idempotency_key = $4`,
		fs.EmailSent, fs.SummaryJSON, time.Now().UTC(), fs.IdempotencyKey,
	)
	if err != nil {
		return fmt.Errorf("storage: update file summary %q: %w", fs.IdempotencyKey, err)
	}
	return nil
}
