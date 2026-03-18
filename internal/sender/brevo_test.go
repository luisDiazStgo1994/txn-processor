package sender

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeTestTemplate creates a minimal email.html in a temp dir and returns its path.
func writeTestTemplate(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	tmpl := `<!DOCTYPE html><html><body>
<p>Balance: ${{printf "%.2f" .TotalBalance}}</p>
{{range .ByYear}}<p>{{.Month}} {{.Year}}: {{.TxnCount}} txns</p>{{end}}
{{if gt .InvalidRows 0}}<p>Invalid: {{.InvalidRows}}</p>{{end}}
</body></html>`
	path := filepath.Join(dir, "email.html")
	if err := os.WriteFile(path, []byte(tmpl), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestBrevoSender_Send_HappyPath(t *testing.T) {
	var capturedBody brevoPayload
	var capturedAPIKey string

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedAPIKey = r.Header.Get("api-key")
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &capturedBody)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"messageId":"ok"}`))
	}))
	defer server.Close()

	tmplPath := writeTestTemplate(t)
	bs, err := NewBrevoSender("test-api-key", "sender@test.com", "TestSender", tmplPath)
	if err != nil {
		t.Fatalf("NewBrevoSender: %v", err)
	}
	// Override the client to hit our test server.
	bs.client = server.Client()
	// We need to override the URL too — use a custom transport.
	bs.client.Transport = rewriteTransport{base: http.DefaultTransport, url: server.URL}

	data := SenderData{
		TotalBalance: 50.2,
		ByYear: []MonthDataDTO{
			{Year: 2026, MonthNum: 7, Month: "July", TxnCount: 2, AvgCredit: 60.5, AvgDebit: -10.3},
		},
		InvalidRows: 0,
	}

	if err := bs.Send(context.Background(), "user@example.com", data); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if capturedAPIKey != "test-api-key" {
		t.Errorf("api-key header = %q, want %q", capturedAPIKey, "test-api-key")
	}
	if capturedBody.Sender.Email != "sender@test.com" {
		t.Errorf("sender email = %q, want %q", capturedBody.Sender.Email, "sender@test.com")
	}
	if len(capturedBody.To) != 1 || capturedBody.To[0].Email != "user@example.com" {
		t.Errorf("to = %v, want [user@example.com]", capturedBody.To)
	}
	if capturedBody.Subject != "Your Stori Transaction Summary" {
		t.Errorf("subject = %q", capturedBody.Subject)
	}
	if !strings.Contains(capturedBody.HTMLContent, "Balance: $50.20") {
		t.Errorf("HTML missing balance; got: %s", capturedBody.HTMLContent[:200])
	}
	if !strings.Contains(capturedBody.HTMLContent, "July 2026: 2 txns") {
		t.Errorf("HTML missing month data")
	}
}

func TestBrevoSender_Send_APIError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		w.Write([]byte(`{"message":"invalid api key"}`))
	}))
	defer server.Close()

	tmplPath := writeTestTemplate(t)
	bs, err := NewBrevoSender("bad-key", "sender@test.com", "Test", tmplPath)
	if err != nil {
		t.Fatalf("NewBrevoSender: %v", err)
	}
	bs.client = server.Client()
	bs.client.Transport = rewriteTransport{base: http.DefaultTransport, url: server.URL}

	err = bs.Send(context.Background(), "user@example.com", SenderData{TotalBalance: 10})
	if err == nil {
		t.Fatal("expected error for 401 response, got nil")
	}
	if !strings.Contains(err.Error(), "401") {
		t.Errorf("error = %v; want to contain status 401", err)
	}
}

func TestBrevoSender_Send_InvalidRowsRendered(t *testing.T) {
	var capturedBody brevoPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &capturedBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	tmplPath := writeTestTemplate(t)
	bs, _ := NewBrevoSender("key", "s@t.com", "S", tmplPath)
	bs.client = server.Client()
	bs.client.Transport = rewriteTransport{base: http.DefaultTransport, url: server.URL}

	data := SenderData{TotalBalance: 100, InvalidRows: 3}
	if err := bs.Send(context.Background(), "u@e.com", data); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(capturedBody.HTMLContent, "Invalid: 3") {
		t.Error("HTML should contain invalid rows count when > 0")
	}
}

func TestBrevoSender_Send_NoInvalidRowsSection(t *testing.T) {
	var capturedBody brevoPayload

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		json.Unmarshal(body, &capturedBody)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	tmplPath := writeTestTemplate(t)
	bs, _ := NewBrevoSender("key", "s@t.com", "S", tmplPath)
	bs.client = server.Client()
	bs.client.Transport = rewriteTransport{base: http.DefaultTransport, url: server.URL}

	data := SenderData{TotalBalance: 100, InvalidRows: 0}
	if err := bs.Send(context.Background(), "u@e.com", data); err != nil {
		t.Fatal(err)
	}

	if strings.Contains(capturedBody.HTMLContent, "Invalid:") {
		t.Error("HTML should NOT contain invalid rows section when InvalidRows=0")
	}
}

func TestNewBrevoSender_BadTemplatePath(t *testing.T) {
	_, err := NewBrevoSender("key", "s@t.com", "S", "/nonexistent/email.html")
	if err == nil {
		t.Fatal("expected error for bad template path, got nil")
	}
}

// rewriteTransport rewrites all request URLs to point at the test server.
type rewriteTransport struct {
	base http.RoundTripper
	url  string
}

func (t rewriteTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.URL.Scheme = "http"
	req.URL.Host = strings.TrimPrefix(t.url, "http://")
	return t.base.RoundTrip(req)
}
