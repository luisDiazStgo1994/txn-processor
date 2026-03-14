// Package email handles HTML template rendering and SMTP delivery.
// It owns its own data types (EmailData) so it has no dependency on
// the aggregator package — the orchestrator bridges the two.
package email

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"math"
	"net/smtp"

	"github.com/luisDiazStgo1994/txn-processor/config"
)

// --- Data types owned by this package ---

// MonthData holds the per-month aggregates rendered in the email template.
type MonthData struct {
	Month     string
	TxnCount  int
	AvgCredit float64
	AvgDebit  float64
}

// EmailData is the payload passed to Send.
// It is populated by the orchestrator from the aggregator's Summary.
type EmailData struct {
	AccountID    string
	RecipientTo  string
	TotalBalance float64
	ByMonth      []MonthData // ordered slice, easier to range in templates
}

// --- Interface ---

// Sender is the email delivery contract.
type Sender interface {
	Send(ctx context.Context, data EmailData) error
}

// --- Implementation ---

// Config groups the SMTP credentials needed by EmailSender.
type Config = config.SMTPConfig

// EmailSender renders an HTML template and delivers it via SMTP.
type EmailSender struct {
	cfg  Config
	tmpl *template.Template
}

// NewEmailSender constructs an EmailSender by loading the HTML template at tmplPath.
func NewEmailSender(cfg Config, tmplPath string) (*EmailSender, error) {
	funcMap := template.FuncMap{
		"absFloat": func(f float64) float64 { return math.Abs(f) },
	}
	tmpl, err := template.New("email.html").Funcs(funcMap).ParseFiles(tmplPath)
	if err != nil {
		return nil, fmt.Errorf("email: parse template %q: %w", tmplPath, err)
	}
	return &EmailSender{cfg: cfg, tmpl: tmpl}, nil
}

// Send renders the HTML template with data and delivers it via SMTP.
// ctx is accepted for interface consistency; SMTP calls are not yet context-aware.
func (s *EmailSender) Send(_ context.Context, data EmailData) error {
	body, err := s.render(data)
	if err != nil {
		return err
	}

	msg := buildMessage(s.cfg.User, data.RecipientTo, "Your Stori Transaction Summary", body)
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	auth := smtp.PlainAuth("", s.cfg.User, s.cfg.Password, s.cfg.Host)

	if err := smtp.SendMail(addr, auth, s.cfg.User, []string{data.RecipientTo}, msg); err != nil {
		return fmt.Errorf("email: send mail to %q: %w", data.RecipientTo, err)
	}
	return nil
}

func (s *EmailSender) render(data EmailData) (string, error) {
	var buf bytes.Buffer
	if err := s.tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("email: render template: %w", err)
	}
	return buf.String(), nil
}

func buildMessage(from, to, subject, htmlBody string) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "From: %s\r\n", from)
	fmt.Fprintf(&buf, "To: %s\r\n", to)
	fmt.Fprintf(&buf, "Subject: %s\r\n", subject)
	fmt.Fprintf(&buf, "MIME-Version: 1.0\r\n")
	fmt.Fprintf(&buf, "Content-Type: text/html; charset=\"UTF-8\"\r\n")
	fmt.Fprintf(&buf, "\r\n%s", htmlBody)
	return buf.Bytes()
}
