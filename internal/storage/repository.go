// Package storage defines the persistence contract and data models
// used across the application.
package storage

import (
	"context"
	"time"
)

// --- Models ---

// Account holds the owner information tied to a set of transactions.
type Account struct {
	AccountID string
	Email     string
}

// FileStatus represents the processing lifecycle of an uploaded file.
type FileStatus string

const (
	FileStatusPending    FileStatus = "pending"
	FileStatusProcessing FileStatus = "processing"
	FileStatusDone       FileStatus = "done"
	FileStatusFailed     FileStatus = "failed"
)

// FileProcessing tracks the state of a single file ingestion run.
// IdempotencyKey is derived from the file (e.g. SHA256 of path or S3 ETag).
type FileProcessing struct {
	IdempotencyKey string
	AccountID      string
	Status         FileStatus
	CheckpointRow  int       // last successfully processed row (enables resumability)
	HeartbeatAt    time.Time // updated periodically; lets other workers detect stale locks
	UpdatedAt      time.Time
}

// FileSummary stores the aggregated result after a file has been fully processed.
// SummaryJSON is stored as JSONB in Postgres so the shape can evolve without migrations.
type FileSummary struct {
	IdempotencyKey string
	AccountID      string
	EmailSent      bool
	SummaryJSON    []byte // JSONB in Postgres
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// --- Repository interface ---

// Repository is the persistence contract.
// Implementations: PostgresRepository (production), MockRepository (tests).
// All methods accept a context to propagate deadlines and cancellations.
type Repository interface {
	// Account
	UpsertAccount(ctx context.Context, account Account) error
	GetAccount(ctx context.Context, accountID string) (Account, error)

	// FileProcessing
	CreateFileProcessing(ctx context.Context, fp FileProcessing) error
	GetFileProcessing(ctx context.Context, idempotencyKey string) (FileProcessing, error)
	UpdateFileProcessing(ctx context.Context, fp FileProcessing) error

	// FileSummary
	CreateFileSummary(ctx context.Context, fs FileSummary) error
	GetFileSummary(ctx context.Context, idempotencyKey string) (FileSummary, error)
	UpdateFileSummary(ctx context.Context, fs FileSummary) error
}
