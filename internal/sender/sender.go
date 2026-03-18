// Package sender defines the email delivery contract and shared data types.
// Implementations: BrevoSender (production), EmailSender (SMTP fallback).
package sender

import "context"

// --- Data types owned by this package ---

// MonthDataDTO holds the per-month aggregates rendered in the email template.
type MonthDataDTO struct {
	Year      int
	MonthNum  int // 1-12, used for chronological sorting
	Month     string
	TxnCount  int
	AvgCredit float64
	AvgDebit  float64
}

// SenderData is the pure content payload passed to Send.
// Routing information (recipient address) is passed separately to Send.
type SenderData struct {
	TotalBalance float64
	ByYear       []MonthDataDTO // ordered slice, easier to range in templates
	InvalidRows  int            // rows that failed parsing; 0 means no issues
}

// --- Interface ---

// Sender is the email delivery contract.
type Sender interface {
	Send(ctx context.Context, to string, data SenderData) error
}
