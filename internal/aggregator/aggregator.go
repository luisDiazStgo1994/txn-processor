// Package aggregator contains the business logic for processing transactions.
// It reads rows via a Parser (stream), computes account summaries, and persists
// processing state to a Repository for idempotency and resumability.
package aggregator

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

// rawRow is the internal struct used by Scan to capture a single CSV row.
// Fields are mapped by the `csv` tag matching the CSV headers.
type rawRow struct {
	ID          int     `csv:"id"`
	Date        string  `csv:"date"`
	Transaction float64 `csv:"transaction"`
}

// Transaction is the domain model for a single processed transaction.
type Transaction struct {
	ID        int
	AccountID string
	Date      time.Time
	Amount    float64 // positive = credit, negative = debit
}

// MonthSummary holds aggregated data for a single calendar month.
// All fields are exported so the struct marshals/unmarshals correctly.
type MonthSummary struct {
	Month     string
	MonthNum  int // 1-12, used for chronological sorting
	TxnCount  int
	AvgCredit float64
	AvgDebit  float64
}

// monthAcc extends MonthSummary with transient counters needed for
// Welford's running average. It is local to processRows and never serialised.
type monthAcc struct {
	MonthSummary
	creditCount int
	debitCount  int
}

// Summary is the result produced by Compute for a single account+file pair.
type Summary struct {
	AccountID    string
	TotalBalance float64
	ByMonth      map[string]MonthSummary
}

// Aggregator reads a CSV file through a Parser, computes the Summary,
// and tracks processing state in the Repository.
type Aggregator struct {
	parser             parser.Parser
	repo               storage.Repository
	accountID          string
	fileKey            string // idempotency key for this file
	checkpointInterval int    // rows between mid-file checkpoint flushes
}

// New creates a ready-to-use Aggregator.
// accountID identifies whose transactions are being processed.
// fileKey is the idempotency key for this file run (e.g. SHA256 or S3 ETag).
// checkpointInterval controls how often the checkpoint row is flushed to the DB.
func New(p parser.Parser, repo storage.Repository, accountID, fileKey string, checkpointInterval int) *Aggregator {
	return &Aggregator{
		parser:             p,
		repo:               repo,
		accountID:          accountID,
		fileKey:            fileKey,
		checkpointInterval: checkpointInterval,
	}
}

// Compute reads all rows from the parser, builds the Summary, and persists
// processing state transitions (pending → processing → done / failed).
// It is safe to call Compute again after a failure — it will resume from
// the recorded checkpoint row.
func (a *Aggregator) Compute(ctx context.Context) (Summary, error) {
	if err := a.parser.ReadHeader(); err != nil {
		return Summary{}, fmt.Errorf("aggregator: read header: %w", err)
	}

	fp, err := a.repo.GetFileProcessing(ctx, a.fileKey)
	if err != nil {
		// First run — create the processing record.
		fp = storage.FileProcessing{
			IdempotencyKey: a.fileKey,
			AccountID:      a.accountID,
			Status:         storage.FileStatusProcessing,
			CheckpointRow:  0,
			HeartbeatAt:    time.Now().UTC(),
		}
		if err := a.repo.CreateFileProcessing(ctx, fp); err != nil {
			return Summary{}, fmt.Errorf("aggregator: create file processing: %w", err)
		}
	} else {
		fp.Status = storage.FileStatusProcessing
		fp.HeartbeatAt = time.Now().UTC()
		if err := a.repo.UpdateFileProcessing(ctx, fp); err != nil {
			return Summary{}, fmt.Errorf("aggregator: update file processing to processing: %w", err)
		}
	}

	summary, err := a.processRows(ctx, fp)
	if err != nil {
		fp.Status = storage.FileStatusFailed
		_ = a.repo.UpdateFileProcessing(ctx, fp)
		return Summary{}, err
	}

	fp.Status = storage.FileStatusDone
	fp.CheckpointRow = summary.txnCount()
	if err := a.repo.UpdateFileProcessing(ctx, fp); err != nil {
		return Summary{}, fmt.Errorf("aggregator: update file processing to done: %w", err)
	}

	return summary, nil
}

// processRows streams rows from the parser and builds the Summary.
// fp is passed by value so we can mutate CheckpointRow locally and flush it
// periodically without affecting the caller's copy until Compute() finalises.
func (a *Aggregator) processRows(ctx context.Context, fp storage.FileProcessing) (Summary, error) {
	summary := Summary{
		AccountID: a.accountID,
		ByMonth:   make(map[string]MonthSummary),
	}
	accs := make(map[string]monthAcc)

	skipRows := fp.CheckpointRow
	var rowNum int
	for {
		select {
		case <-ctx.Done():
			return Summary{}, fmt.Errorf("aggregator: context cancelled: %w", ctx.Err())
		default:
		}

		var row rawRow
		err := a.parser.Scan(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return Summary{}, fmt.Errorf("aggregator: scan row %d: %w", rowNum, err)
		}

		rowNum++
		if rowNum <= skipRows {
			continue // resume past already-processed rows
		}

		txn, err := parseRow(row, a.accountID)
		if err != nil {
			// Malformed rows are skipped with a note; they don't abort the run.
			// TODO: emit a structured log here when observability is added.
			_ = fmt.Sprintf("aggregator: skipping malformed row %d: %v", rowNum, err)
			continue
		}

		summary.TotalBalance += txn.Amount
		key := txn.Date.Format("January")
		acc := accs[key]
		acc.apply(txn)
		accs[key] = acc

		// Flush a mid-file checkpoint so a crash loses at most checkpointInterval rows.
		if rowNum%a.checkpointInterval == 0 {
			fp.CheckpointRow = rowNum
			fp.HeartbeatAt = time.Now().UTC()
			_ = a.repo.UpdateFileProcessing(ctx, fp) // best-effort; don't abort on flush error
		}
	}

	for k, acc := range accs {
		summary.ByMonth[k] = acc.MonthSummary
	}

	return summary, nil
}

// parseRow converts a rawRow into a domain Transaction.
func parseRow(row rawRow, accountID string) (Transaction, error) {
	date, err := parseDate(row.Date)
	if err != nil {
		return Transaction{}, fmt.Errorf("parse date %q: %w", row.Date, err)
	}
	return Transaction{
		ID:        row.ID,
		AccountID: accountID,
		Date:      date,
		Amount:    row.Transaction,
	}, nil
}

// parseDate parses a date in M/D format, using the current year.
func parseDate(raw string) (time.Time, error) {
	parts := strings.Split(raw, "/")
	if len(parts) != 2 {
		return time.Time{}, fmt.Errorf("expected M/D, got %q", raw)
	}
	month, err := strconv.Atoi(parts[0])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid month: %w", err)
	}
	day, err := strconv.Atoi(parts[1])
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid day: %w", err)
	}
	return time.Date(time.Now().Year(), time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}

// apply updates the accumulator with a single transaction using Welford's
// running average so we never need to store all amounts in memory.
func (a *monthAcc) apply(txn Transaction) {
	a.Month = txn.Date.Format("January")
	a.MonthNum = int(txn.Date.Month())
	a.TxnCount++

	if txn.Amount > 0 {
		a.creditCount++
		// Welford-style running average: avgnew = avgold + (x - avgold) / n
		a.AvgCredit += (txn.Amount - a.AvgCredit) / float64(a.creditCount)
	} else {
		a.debitCount++
		a.AvgDebit += (txn.Amount - a.AvgDebit) / float64(a.debitCount)
	}
}

// txnCount returns the total number of transactions across all months.
func (s Summary) txnCount() int {
	total := 0
	for _, ms := range s.ByMonth {
		total += ms.TxnCount
	}
	return total
}
