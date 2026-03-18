package sender

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"math"
	"net/smtp"
)

// SMTPConfig groups the credentials needed by EmailSender.
type SMTPConfig struct {
	Host     string
	Port     int
	User     string
	Password string
}

// EmailSender renders an HTML template and delivers it via SMTP.
type EmailSender struct {
	cfg  SMTPConfig
	tmpl *template.Template
}

// NewEmailSender constructs an EmailSender by loading the HTML template at tmplPath.
func NewEmailSender(cfg SMTPConfig, tmplPath string) (*EmailSender, error) {
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
func (s *EmailSender) Send(_ context.Context, to string, data SenderData) error {
	body, err := s.render(data)
	if err != nil {
		return err
	}

	msg := buildMessage(s.cfg.User, to, "Your Stori Transaction Summary", body)
	addr := fmt.Sprintf("%s:%d", s.cfg.Host, s.cfg.Port)
	auth := smtp.PlainAuth("", s.cfg.User, s.cfg.Password, s.cfg.Host)

	if err := smtp.SendMail(addr, auth, s.cfg.User, []string{to}, msg); err != nil {
		return fmt.Errorf("email: send mail to %q: %w", to, err)
	}
	return nil
}

func (s *EmailSender) render(data SenderData) (string, error) {
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
