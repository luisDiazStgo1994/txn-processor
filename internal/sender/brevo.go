package sender

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"net/http"
)

// BrevoSender renders an HTML template and delivers it via the Brevo transactional email API.
type BrevoSender struct {
	apiKey     string
	senderName string
	senderEmail string
	tmpl       *template.Template
	client     *http.Client
}

// NewBrevoSender constructs a BrevoSender by loading the HTML template at tmplPath.
func NewBrevoSender(apiKey, senderEmail, senderName, tmplPath string) (*BrevoSender, error) {
	funcMap := template.FuncMap{
		"absFloat": func(f float64) float64 { return math.Abs(f) },
	}
	tmpl, err := template.New("email.html").Funcs(funcMap).ParseFiles(tmplPath)
	if err != nil {
		return nil, fmt.Errorf("brevo: parse template %q: %w", tmplPath, err)
	}
	return &BrevoSender{
		apiKey:      apiKey,
		senderName:  senderName,
		senderEmail: senderEmail,
		tmpl:        tmpl,
		client:      &http.Client{},
	}, nil
}

// brevoPayload is the JSON body for the Brevo transactional email API.
type brevoPayload struct {
	Sender      brevoContact   `json:"sender"`
	To          []brevoContact `json:"to"`
	Subject     string         `json:"subject"`
	HTMLContent string         `json:"htmlContent"`
}

type brevoContact struct {
	Name  string `json:"name,omitempty"`
	Email string `json:"email"`
}

// Send renders the HTML template and delivers it via the Brevo API.
func (s *BrevoSender) Send(ctx context.Context, to string, data SenderData) error {
	html, err := s.render(data)
	if err != nil {
		return err
	}

	payload := brevoPayload{
		Sender:      brevoContact{Name: s.senderName, Email: s.senderEmail},
		To:          []brevoContact{{Email: to}},
		Subject:     "Your Stori Transaction Summary",
		HTMLContent: html,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("brevo: marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.brevo.com/v3/smtp/email", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("brevo: create request: %w", err)
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("content-type", "application/json")
	req.Header.Set("api-key", s.apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("brevo: send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		var errBody bytes.Buffer
		errBody.ReadFrom(resp.Body)
		return fmt.Errorf("brevo: API returned %d: %s", resp.StatusCode, errBody.String())
	}

	return nil
}

func (s *BrevoSender) render(data SenderData) (string, error) {
	var buf bytes.Buffer
	if err := s.tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("brevo: render template: %w", err)
	}
	return buf.String(), nil
}
