// Package aggregator contains the business logic for processing transactions.
// It reads rows via a Parser (stream), computes account summaries, and persists
// processing state to a Repository for idempotency and resumability.
package aggregator

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

// Transaction is the domain model for a single processed transaction.
type Transaction struct {
	ID     int
	Date   time.Time
	Amount float64 // positive = credit, negative = debit
}

// monthSummary holds aggregated data for a single calendar month.
// All fields are exported so the struct marshals/unmarshals correctly
// (required for checkpoint resumability).
type monthSummary struct {
	Month       string
	MonthNum    int // 1-12, used for chronological sorting
	TxnCount    int
	AvgCredit   float64
	AvgDebit    float64
	CreditCount int // exported so Welford state survives JSON round-trips
	DebitCount  int // exported so Welford state survives JSON round-trips
}

// yearSummary holds all monthly aggregates for a single calendar year.
type yearSummary struct {
	TxnCount int
	ByMonth  map[string]monthSummary // key: month name ("January", "February", ...)
}

// Summary is the result produced by Compute for a single account+file pair.
type Summary struct {
	AccountID    string
	TotalBalance float64
	ByYear       map[string]yearSummary // key: year string ("2026", ...)
}

// RowError records a single parse/validation failure during processing.
type RowError struct {
	Row   int    `json:"row"`
	Error string `json:"error"`
}

// ErrTooManyRowErrors is returned when the number of malformed rows exceeds the
// configured threshold. The caller should mark the file as to_review.
var ErrTooManyRowErrors = errors.New("aggregator: row error threshold exceeded")

// Aggregator reads a CSV file through a Parser, computes the Summary,
// and tracks processing state in the Repository.
type Aggregator struct {
	parser             parser.Parser
	repo               storage.Repository
	accountID          string
	fileKey            string        // idempotency key for this file
	checkpointInterval int           // rows between mid-file checkpoint flushes
	heartbeatTimeout   time.Duration // stale lock reclaim threshold
	maxRowErrors       int           // max tolerated parse errors before to_review
}

// New creates a ready-to-use Aggregator.
func New(p parser.Parser, repo storage.Repository, accountID, fileKey string, checkpointInterval, heartbeatTimeoutSecs, maxRowErrors int) *Aggregator {
	return &Aggregator{
		parser:             p,
		repo:               repo,
		accountID:          accountID,
		fileKey:            fileKey,
		checkpointInterval: checkpointInterval,
		heartbeatTimeout:   time.Duration(heartbeatTimeoutSecs) * time.Second,
		maxRowErrors:       maxRowErrors,
	}
}

// Compute reads all rows from the parser, builds the Summary, and persists
// processing state transitions (pending → processing → done / failed).
// It is safe to call Compute again after a failure — it will resume from
// the recorded checkpoint row.
func (a *Aggregator) Compute(ctx context.Context) (Summary, error) {
	summary := Summary{
		AccountID: a.accountID,
		ByYear:    make(map[string]yearSummary),
	}

	if err := a.parser.ReadHeader(); err != nil {
		return summary, fmt.Errorf("aggregator: read header: %w", err)
	}

	fp, err := a.repo.GetFileProcessing(ctx, a.fileKey)
	if errors.Is(err, sql.ErrNoRows) {
		// First run — create the processing record.
		fp = storage.FileProcessingRow{
			IdempotencyKey: a.fileKey,
			AccountID:      a.accountID,
			Status:         storage.FileStatusProcessing,
			CheckpointRow:  0,
			HeartbeatAt:    time.Now().UTC(),
		}
		if err := a.repo.CreateFileProcessing(ctx, fp); err != nil {
			return summary, fmt.Errorf("aggregator: create file processing: %w", err)
		}
	} else if err != nil {
		return summary, fmt.Errorf("aggregator: get file processing: %w", err)
	} else {
		if fp.Status == storage.FileStatusDone {
			return summary, fmt.Errorf("aggregator: file already processed")
		}
		if fp.Status == storage.FileStatusProcessing {
			if time.Since(fp.HeartbeatAt) < a.heartbeatTimeout {
				return summary, fmt.Errorf("aggregator: file already being processed")
			}
			slog.Warn("stale processing lock detected, taking over",
				"fileKey", a.fileKey,
				"lastHeartbeat", fp.HeartbeatAt,
				"threshold", a.heartbeatTimeout,
			)
		}
		// Status is failed, or processing lock is stale — resume from last checkpoint.
		fp.Status = storage.FileStatusProcessing
		fp.HeartbeatAt = time.Now().UTC()
		if err := a.repo.UpdateFileProcessing(ctx, fp); err != nil {
			return summary, fmt.Errorf("aggregator: update file processing to processing: %w", err)
		}

		// Load the partial summary saved at the last checkpoint.
		// If no checkpoint was ever saved (failed before first flush), start fresh.
		fileSummaryRow, err := a.repo.GetFileSummary(ctx, a.fileKey)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return summary, fmt.Errorf("aggregator: get file summary from db: %w", err)
		}
		if fileSummaryRow.SummaryJSON != nil {
			if err := json.Unmarshal(fileSummaryRow.SummaryJSON, &summary); err != nil {
				return summary, fmt.Errorf("aggregator: unmarshal summary json from db: %w", err)
			}
		}
	}

	rowErrors, err := a.processRows(ctx, fp, &summary)
	if err != nil {
		if errors.Is(err, ErrTooManyRowErrors) {
			fp.Status = storage.FileStatusToReview
			if errJSON, marshalErr := json.Marshal(rowErrors); marshalErr == nil {
				fp.RowErrorsJSON = errJSON
			}
		} else {
			fp.Status = storage.FileStatusFailed
		}
		_ = a.repo.UpdateFileProcessing(ctx, fp)
		return summary, err
	}

	fp.Status = storage.FileStatusDone
	fp.CheckpointRow = summary.txnCount()
	if err := a.repo.UpdateFileProcessing(ctx, fp); err != nil {
		return summary, fmt.Errorf("aggregator: update file processing to done: %w", err)
	}

	return summary, nil
}

// processRows streams rows from the parser and builds the Summary.
// It returns the accumulated row errors and a non-nil error if the threshold
// is exceeded (ErrTooManyRowErrors) or if the context is cancelled.
func (a *Aggregator) processRows(ctx context.Context, fp storage.FileProcessingRow, summary *Summary) ([]RowError, error) {
	skipRows := fp.CheckpointRow
	var rowNum int
	var rowErrors []RowError

	for {
		select {
		case <-ctx.Done():
			return rowErrors, fmt.Errorf("aggregator: context cancelled: %w", ctx.Err())
		default:
		}

		var row parser.TransactionRow
		err := a.parser.Scan(&row)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			rowErrors = append(rowErrors, RowError{Row: rowNum, Error: err.Error()})
			slog.Warn("malformed row", "row", rowNum, "error", err, "total_errors", len(rowErrors))
			if len(rowErrors) > a.maxRowErrors {
				return rowErrors, ErrTooManyRowErrors
			}
			continue
		}

		rowNum++
		if rowNum <= skipRows {
			continue // resume past already-processed rows
		}

		txn := Transaction{
			ID:     row.ID,
			Date:   row.Date,
			Amount: row.Amount,
		}

		summary.TotalBalance += txn.Amount
		yearKey := txn.Date.Format("2006")
		ys := summary.ByYear[yearKey]
		ys.apply(txn)
		summary.ByYear[yearKey] = ys

		// Flush a mid-file checkpoint so a crash loses at most checkpointInterval rows.
		if rowNum%a.checkpointInterval == 0 {
			fp.CheckpointRow = rowNum
			fp.HeartbeatAt = time.Now().UTC()
			if err := a.repo.UpdateFileProcessing(ctx, fp); err != nil {
				slog.Warn("checkpoint: update file processing", "row", rowNum, "error", err)
			}
			summaryBs, err := json.Marshal(summary)
			if err != nil {
				slog.Warn("checkpoint: marshal summary", "row", rowNum, "error", err)
				continue
			}
			if err := a.PersistSummary(ctx, a.fileKey, a.accountID, summaryBs); err != nil {
				slog.Warn("checkpoint: persist summary", "row", rowNum, "error", err)
			}
		}
	}

	return rowErrors, nil
}

// PersistSummary upserts the FileSummary row (create on first run, update on retry).
func (a *Aggregator) PersistSummary(ctx context.Context, fileKey, accountID string, summaryJSON []byte) error {
	existing, err := a.repo.GetFileSummary(ctx, fileKey)
	if errors.Is(err, sql.ErrNoRows) {
		// First run — create.
		return a.repo.CreateFileSummary(ctx, storage.FileSummaryRow{
			IdempotencyKey: fileKey,
			AccountID:      accountID,
			EmailSent:      false,
			SummaryJSON:    summaryJSON,
			CreatedAt:      time.Now().UTC(),
			UpdatedAt:      time.Now().UTC(),
		})
	}
	if err != nil {
		return fmt.Errorf("aggregator: get file summary: %w", err)
	}
	if existing.EmailSent {
		return nil // nothing to update; pipeline already completed
	}
	existing.SummaryJSON = summaryJSON
	existing.UpdatedAt = time.Now().UTC()
	return a.repo.UpdateFileSummary(ctx, existing)
}

// apply updates the yearSummary with a single transaction using Welford's
// running average so we never need to store all amounts in memory.
func (ys *yearSummary) apply(txn Transaction) {
	if ys.ByMonth == nil {
		ys.ByMonth = make(map[string]monthSummary)
	}
	key := txn.Date.Format("January")
	ms := ys.ByMonth[key]
	ms.Month = key
	ms.MonthNum = int(txn.Date.Month())
	ms.TxnCount++
	ys.TxnCount++

	if txn.Amount > 0 {
		ms.CreditCount++
		// Welford-style running average: avgnew = avgold + (x - avgold) / n
		ms.AvgCredit += (txn.Amount - ms.AvgCredit) / float64(ms.CreditCount)
	} else {
		ms.DebitCount++
		ms.AvgDebit += (txn.Amount - ms.AvgDebit) / float64(ms.DebitCount)
	}

	ys.ByMonth[key] = ms
}

// txnCount returns the total number of transactions across all years.
func (s Summary) txnCount() int {
	total := 0
	for _, ys := range s.ByYear {
		total += ys.TxnCount
	}
	return total
}
