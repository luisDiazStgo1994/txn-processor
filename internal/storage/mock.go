package storage

import (
	"context"
	"database/sql"
	"fmt"
)

// MockRepository is an in-memory Repository for use in unit tests.
// It satisfies the Repository interface without requiring a real database.
type MockRepository struct {
	accounts       map[string]Account
	fileProcessing map[string]FileProcessingRow
	fileSummaries  map[string]FileSummaryRow
}

// NewMockRepository returns an initialized MockRepository.
func NewMockRepository() *MockRepository {
	return &MockRepository{
		accounts:       make(map[string]Account),
		fileProcessing: make(map[string]FileProcessingRow),
		fileSummaries:  make(map[string]FileSummaryRow),
	}
}

// --- Account ---

func (m *MockRepository) UpsertAccount(_ context.Context, account Account) error {
	m.accounts[account.AccountID] = account
	return nil
}

func (m *MockRepository) GetAccount(_ context.Context, accountID string) (Account, error) {
	a, ok := m.accounts[accountID]
	if !ok {
		return Account{}, fmt.Errorf("storage: account %q not found: %w", accountID, sql.ErrNoRows)
	}
	return a, nil
}

// --- FileProcessing ---

func (m *MockRepository) CreateFileProcessing(_ context.Context, fp FileProcessingRow) error {
	if _, exists := m.fileProcessing[fp.IdempotencyKey]; exists {
		return fmt.Errorf("storage: file processing %q already exists", fp.IdempotencyKey)
	}
	m.fileProcessing[fp.IdempotencyKey] = fp
	return nil
}

func (m *MockRepository) GetFileProcessing(_ context.Context, idempotencyKey string) (FileProcessingRow, error) {
	fp, ok := m.fileProcessing[idempotencyKey]
	if !ok {
		return FileProcessingRow{}, fmt.Errorf("storage: file processing %q not found: %w", idempotencyKey, sql.ErrNoRows)
	}
	return fp, nil
}

func (m *MockRepository) UpdateFileProcessing(_ context.Context, fp FileProcessingRow) error {
	if _, ok := m.fileProcessing[fp.IdempotencyKey]; !ok {
		return fmt.Errorf("storage: file processing %q not found", fp.IdempotencyKey)
	}
	m.fileProcessing[fp.IdempotencyKey] = fp
	return nil
}

// --- FileSummary ---

func (m *MockRepository) CreateFileSummary(_ context.Context, fs FileSummaryRow) error {
	if _, exists := m.fileSummaries[fs.IdempotencyKey]; exists {
		return fmt.Errorf("storage: file summary %q already exists", fs.IdempotencyKey)
	}
	m.fileSummaries[fs.IdempotencyKey] = fs
	return nil
}

func (m *MockRepository) GetFileSummary(_ context.Context, idempotencyKey string) (FileSummaryRow, error) {
	fs, ok := m.fileSummaries[idempotencyKey]
	if !ok {
		return FileSummaryRow{}, fmt.Errorf("storage: file summary %q not found: %w", idempotencyKey, sql.ErrNoRows)
	}
	return fs, nil
}

func (m *MockRepository) UpdateFileSummary(_ context.Context, fs FileSummaryRow) error {
	if _, ok := m.fileSummaries[fs.IdempotencyKey]; !ok {
		return fmt.Errorf("storage: file summary %q not found", fs.IdempotencyKey)
	}
	m.fileSummaries[fs.IdempotencyKey] = fs
	return nil
}
