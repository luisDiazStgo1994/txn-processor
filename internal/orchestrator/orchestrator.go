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
	"net/mail"
	"sort"
	"strconv"
	"time"

	"github.com/luisDiazStgo1994/txn-processor/config"
	"github.com/luisDiazStgo1994/txn-processor/internal/aggregator"
	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
	"github.com/luisDiazStgo1994/txn-processor/internal/sender"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

// Orchestrator wires the sender and repository together and drives the pipeline.
type Orchestrator struct {
	sender sender.Sender
	repo   storage.Repository
	config config.AppConfig
}

// New creates a ready-to-use Orchestrator.
func New(repo storage.Repository, sender sender.Sender, config config.AppConfig) *Orchestrator {
	return &Orchestrator{repo: repo, sender: sender, config: config}
}

// Run executes the full pipeline for a single file:
//  1. Get the account and validate its email
//  2. Derive a stable idempotency key from filePath
//  3. Build a CsvParser from src and run the aggregator
//  4. Persist the summary
//  5. Send the email and mark it as sent
//
// src is an io.Reader so callers can pass a local file, an S3 stream, or any
// other source — the orchestrator stays agnostic to the origin.
func (o *Orchestrator) Run(ctx context.Context, p parser.Parser, accountID, filePath string) error {
	account, err := o.repo.GetAccount(ctx, accountID)
	if err != nil {
		return fmt.Errorf("orchestrator: get account: %w", err)
	}

	if _, err := mail.ParseAddress(account.Email); err != nil {
		return fmt.Errorf("orchestrator: invalid email for account %q: %w", accountID, err)
	}

	fileKey := idempotencyKey(filePath, accountID)

	agg := aggregator.New(p, o.repo, accountID, fileKey, o.config.CheckpointInterval, o.config.HeartbeatTimeoutSecs, o.config.MaxRowErrors)

	summary, err := agg.Compute(ctx)
	if err != nil {
		return fmt.Errorf("orchestrator: compute: %w", err)
	}

	summaryJSON, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("orchestrator: marshal summary: %w", err)
	}

	if err := agg.PersistSummary(ctx, fileKey, accountID, summaryJSON); err != nil {
		return err
	}

	fs, err := o.repo.GetFileSummary(ctx, fileKey)
	if err != nil {
		return fmt.Errorf("orchestrator: get file summary: %w", err)
	}
	if fs.EmailSent {
		return nil // idempotent — email already delivered on a previous run
	}

	if err := o.sender.Send(ctx, account.Email, toSenderData(summary)); err != nil {
		return fmt.Errorf("orchestrator: send email: %w", err)
	}

	fs.EmailSent = true
	fs.UpdatedAt = time.Now().UTC()
	if err := o.repo.UpdateFileSummary(ctx, fs); err != nil {
		return fmt.Errorf("orchestrator: mark email sent: %w", err)
	}

	return nil
}

// idempotencyKey returns a short, stable key derived from a file path.
// For S3-triggered flows, use the object ETag instead.
func idempotencyKey(path, accountId string) string {
	h := sha256.Sum256([]byte(fmt.Sprintf("%s_%s", accountId, path)))
	return fmt.Sprintf("%x", h[:8])
}

// toSenderData translates an aggregator.Summary into the email package's DTO.
// Flattens ByYear→ByMonth into a flat slice sorted chronologically (year then month).
func toSenderData(s aggregator.Summary) sender.SenderData {
	var months []sender.MonthDataDTO
	for yearKey, ys := range s.ByYear {
		year, _ := strconv.Atoi(yearKey)
		for _, ms := range ys.ByMonth {
			months = append(months, sender.MonthDataDTO{
				Year:      year,
				MonthNum:  ms.MonthNum,
				Month:     ms.Month,
				TxnCount:  ms.TxnCount,
				AvgCredit: ms.AvgCredit,
				AvgDebit:  ms.AvgDebit,
			})
		}
	}
	sort.Slice(months, func(i, j int) bool {
		ki := months[i].Year*12 + months[i].MonthNum
		kj := months[j].Year*12 + months[j].MonthNum
		return ki < kj
	})
	return sender.SenderData{
		TotalBalance: s.TotalBalance,
		ByYear:       months,
	}
}

