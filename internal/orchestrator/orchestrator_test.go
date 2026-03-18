package orchestrator_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/luisDiazStgo1994/txn-processor/config"
	"github.com/luisDiazStgo1994/txn-processor/internal/orchestrator"
	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
	"github.com/luisDiazStgo1994/txn-processor/internal/sender"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

// mockSender captures sent emails and can be configured to return an error.
type mockSender struct {
	sent []sender.SenderData
	to   []string
	err  error
}

func (m *mockSender) Send(_ context.Context, to string, data sender.SenderData) error {
	if m.err != nil {
		return m.err
	}
	m.to = append(m.to, to)
	m.sent = append(m.sent, data)
	return nil
}

const csvData = "id,date,transaction\n0,15/07/2026,+60.5\n1,28/07/2026,-10.3\n"

func defaultConfig() config.AppConfig {
	return config.AppConfig{
		CheckpointInterval:   100,
		HeartbeatTimeoutSecs: 20,
		MaxRowErrors:         10,
	}
}

func seedAccount(repo *storage.MockRepository) {
	repo.UpsertAccount(context.Background(), storage.Account{
		AccountID: "ACC-001",
		Email:     "user@example.com",
	})
}

func TestRun_HappyPath(t *testing.T) {
	repo := storage.NewMockRepository()
	seedAccount(repo)
	ms := &mockSender{}
	orch := orchestrator.New(repo, ms, defaultConfig())

	p := parser.NewCsvParser(strings.NewReader(csvData))
	if err := orch.Run(context.Background(), p, "ACC-001", "txns.csv"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(ms.sent) != 1 {
		t.Fatalf("Send called %d times, want 1", len(ms.sent))
	}
	if ms.to[0] != "user@example.com" {
		t.Errorf("Send to = %q, want %q", ms.to[0], "user@example.com")
	}

	// Balance should be 60.5 + (-10.3) = 50.2
	got := ms.sent[0].TotalBalance
	want := 50.2
	if got < want-0.01 || got > want+0.01 {
		t.Errorf("TotalBalance = %v, want %v", got, want)
	}
}

func TestRun_IdempotentSecondRunNoOp(t *testing.T) {
	repo := storage.NewMockRepository()
	seedAccount(repo)

	// First run: processes file and sends email.
	ms1 := &mockSender{}
	orch1 := orchestrator.New(repo, ms1, defaultConfig())
	p1 := parser.NewCsvParser(strings.NewReader(csvData))
	if err := orch1.Run(context.Background(), p1, "ACC-001", "txns.csv"); err != nil {
		t.Fatalf("first run error: %v", err)
	}
	if len(ms1.sent) != 1 {
		t.Fatalf("first run: Send called %d times, want 1", len(ms1.sent))
	}

	// Second run with same file: idempotent no-op, no error, no email resend.
	ms2 := &mockSender{}
	orch2 := orchestrator.New(repo, ms2, defaultConfig())
	p2 := parser.NewCsvParser(strings.NewReader(csvData))
	if err := orch2.Run(context.Background(), p2, "ACC-001", "txns.csv"); err != nil {
		t.Fatalf("second run should be a no-op, got error: %v", err)
	}
	if len(ms2.sent) != 0 {
		t.Errorf("second run: Send called %d times, want 0", len(ms2.sent))
	}
}

func TestRun_EmailSendFailure(t *testing.T) {
	repo := storage.NewMockRepository()
	seedAccount(repo)
	sendErr := errors.New("brevo: connection refused")
	ms := &mockSender{err: sendErr}
	orch := orchestrator.New(repo, ms, defaultConfig())

	p := parser.NewCsvParser(strings.NewReader(csvData))
	err := orch.Run(context.Background(), p, "ACC-001", "txns.csv")
	if err == nil {
		t.Fatal("expected error from failed email send, got nil")
	}
	if !errors.Is(err, sendErr) {
		t.Errorf("error = %v; want to wrap %v", err, sendErr)
	}
}

func TestRun_EmailDataMatchesSummary(t *testing.T) {
	repo := storage.NewMockRepository()
	seedAccount(repo)
	ms := &mockSender{}
	orch := orchestrator.New(repo, ms, defaultConfig())

	p := parser.NewCsvParser(strings.NewReader(csvData))
	if err := orch.Run(context.Background(), p, "ACC-001", "txns.csv"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data := ms.sent[0]
	if len(data.ByYear) != 1 {
		t.Fatalf("ByYear len = %d, want 1", len(data.ByYear))
	}
	if data.ByYear[0].MonthNum != 7 {
		t.Errorf("first month MonthNum = %d, want 7", data.ByYear[0].MonthNum)
	}
	if data.ByYear[0].Month != "July" {
		t.Errorf("first month = %q, want July", data.ByYear[0].Month)
	}
}

func TestRun_InvalidAccountEmail(t *testing.T) {
	repo := storage.NewMockRepository()
	repo.UpsertAccount(context.Background(), storage.Account{
		AccountID: "ACC-001",
		Email:     "not-an-email",
	})
	ms := &mockSender{}
	orch := orchestrator.New(repo, ms, defaultConfig())

	p := parser.NewCsvParser(strings.NewReader(csvData))
	err := orch.Run(context.Background(), p, "ACC-001", "txns.csv")
	if err == nil {
		t.Fatal("expected error for invalid email, got nil")
	}
	if len(ms.sent) != 0 {
		t.Error("Send should not be called when email is invalid")
	}
}

func TestRun_AccountNotFound(t *testing.T) {
	repo := storage.NewMockRepository()
	ms := &mockSender{}
	orch := orchestrator.New(repo, ms, defaultConfig())

	p := parser.NewCsvParser(strings.NewReader(csvData))
	err := orch.Run(context.Background(), p, "MISSING", "txns.csv")
	if err == nil {
		t.Fatal("expected error for missing account, got nil")
	}
}

func TestRun_InvalidRowsReported(t *testing.T) {
	// One valid row, one with bad date
	csv := "id,date,transaction\n0,15/07/2026,+60.5\n1,baddate,-10.3\n"
	repo := storage.NewMockRepository()
	seedAccount(repo)
	ms := &mockSender{}
	orch := orchestrator.New(repo, ms, defaultConfig())

	p := parser.NewCsvParser(strings.NewReader(csv))
	if err := orch.Run(context.Background(), p, "ACC-001", "txns.csv"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if ms.sent[0].InvalidRows != 1 {
		t.Errorf("InvalidRows = %d, want 1", ms.sent[0].InvalidRows)
	}
}
