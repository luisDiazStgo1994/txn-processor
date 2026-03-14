// Package orchestrator coordinates the full processing pipeline:
//  1. Derive idempotency key and ensure the account exists
//  2. Run the aggregator to compute the summary
//  3. Persist the summary as JSONB
//  4. Send the summary email
//  5. Mark email_sent = true
package orchestrator

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/luisDiazStgo1994/txn-processor/internal/aggregator"
	"github.com/luisDiazStgo1994/txn-processor/internal/email"
	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

// Orchestrator wires the sender and repository together and drives the pipeline.
type Orchestrator struct {
	sender email.Sender
	repo   storage.Repository
}

// New creates a ready-to-use Orchestrator.
func New(repo storage.Repository, sender email.Sender) *Orchestrator {
	return &Orchestrator{repo: repo, sender: sender}
}

// Run executes the full pipeline for a single file:
//  1. Upsert the account (ensure it exists before any processing)
//  2. Derive a stable idempotency key from filePath
//  3. Build a CsvParser from src and run the aggregator
//  4. Persist the summary
//  5. Send the email and mark it as sent
//
// src is an io.Reader so callers can pass a local file, an S3 stream, or any
// other source — the orchestrator stays agnostic to the origin.
func (o *Orchestrator) Run(ctx context.Context, p parser.Parser, filePath, accountID, recipientEmail string) error {
	if err := o.repo.UpsertAccount(ctx, storage.Account{
		AccountID: accountID,
		Email:     recipientEmail,
	}); err != nil {
		return fmt.Errorf("orchestrator: upsert account: %w", err)
	}

	fileKey := idempotencyKey(filePath)

	agg := aggregator.New(p, o.repo, accountID, fileKey)

	summary, err := agg.Compute(ctx)
	if err != nil {
		return fmt.Errorf("orchestrator: compute: %w", err)
	}

	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("orchestrator: marshal summary: %w", err)
	}

	if err := o.persistSummary(ctx, fileKey, accountID, summaryJSON); err != nil {
		return err
	}

	fs, err := o.repo.GetFileSummary(ctx, fileKey)
	if err != nil {
		return fmt.Errorf("orchestrator: get file summary: %w", err)
	}
	if fs.EmailSent {
		return nil // idempotent — email already delivered on a previous run
	}

	if err := o.sender.Send(ctx, toEmailData(summary, recipientEmail)); err != nil {
		return fmt.Errorf("orchestrator: send email: %w", err)
	}

	fs.EmailSent = true
	fs.UpdatedAt = time.Now().UTC()
	if err := o.repo.UpdateFileSummary(ctx, fs); err != nil {
		return fmt.Errorf("orchestrator: mark email sent: %w", err)
	}

	return nil
}

// persistSummary upserts the FileSummary row (create on first run, update on retry).
func (o *Orchestrator) persistSummary(ctx context.Context, fileKey, accountID string, summaryJSON []byte) error {
	existing, err := o.repo.GetFileSummary(ctx, fileKey)
	if err != nil {
		// First run — create.
		return o.repo.CreateFileSummary(ctx, storage.FileSummary{
			IdempotencyKey: fileKey,
			AccountID:      accountID,
			EmailSent:      false,
			SummaryJSON:    summaryJSON,
			CreatedAt:      time.Now().UTC(),
			UpdatedAt:      time.Now().UTC(),
		})
	}
	if existing.EmailSent {
		return nil // nothing to update; pipeline already completed
	}
	existing.SummaryJSON = summaryJSON
	existing.UpdatedAt = time.Now().UTC()
	return o.repo.UpdateFileSummary(ctx, existing)
}

// idempotencyKey returns a short, stable key derived from a file path.
// For S3-triggered flows, use the object ETag instead.
func idempotencyKey(path string) string {
	h := sha256.Sum256([]byte(path))
	return fmt.Sprintf("%x", h[:8])
}

// toEmailData translates an aggregator.Summary into the email package's DTO.
// Months are sorted alphabetically for consistent email rendering.
func toEmailData(s aggregator.Summary, recipientEmail string) email.EmailData {
	months := make([]email.MonthData, 0, len(s.ByMonth))
	for _, ms := range s.ByMonth {
		months = append(months, email.MonthData{
			MonthNum:  ms.MonthNum,
			Month:     ms.Month,
			TxnCount:  ms.TxnCount,
			AvgCredit: ms.AvgCredit,
			AvgDebit:  ms.AvgDebit,
		})
	}
	sort.Slice(months, func(i, j int) bool {
		return months[i].MonthNum < months[j].MonthNum
	})
	return email.EmailData{
		AccountID:    s.AccountID,
		RecipientTo:  recipientEmail,
		TotalBalance: s.TotalBalance,
		ByMonth:      months,
	}
}
