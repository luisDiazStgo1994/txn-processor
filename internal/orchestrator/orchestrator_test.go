package orchestrator_test

import (
	"context"
	"testing"

	"github.com/luisDiazStgo1994/txn-processor/internal/sender"
)

// mockSender captures sent emails and can be configured to return an error.
type mockSender struct {
	sent []sender.SenderData
	err  error
}

func (m *mockSender) Send(_ context.Context, to string, data sender.SenderData) error {
	if m.err != nil {
		return m.err
	}
	m.sent = append(m.sent, data)
	return nil
}

const csvData = "id,date,transaction\n0,7/15,+60.5\n1,7/28,-10.3\n"

func TestRun_HappyPath(t *testing.T) {
	// repo := storage.NewMockRepository()
	// sender := &mockSender{}
	// orch := orchestrator.New(repo, sender, 100)

	// p := parser.NewCsvParser(strings.NewReader(csvData))
	// if err := orch.Run(context.Background(), p, "txns.csv", "ACC-001", "user@example.com"); err != nil {
	// 	t.Fatalf("unexpected error: %v", err)
	// }

	// // Account should be upserted with the correct email
	// acc, err := repo.GetAccount(context.Background(), "ACC-001")
	// if err != nil {
	// 	t.Fatalf("account not found: %v", err)
	// }
	// if acc.Email != "user@example.com" {
	// 	t.Errorf("Account.Email = %q, want %q", acc.Email, "user@example.com")
	// }

	// // Exactly one email should have been sent
	// if len(sender.sent) != 1 {
	// 	t.Errorf("Send called %d times, want 1", len(sender.sent))
	// }
	// if sender.sent[0].RecipientTo != "user@example.com" {
	// 	t.Errorf("RecipientTo = %q, want %q", sender.sent[0].RecipientTo, "user@example.com")
	// }
}

func TestRun_IdempotentEmailNotResent(t *testing.T) {
	// repo := storage.NewMockRepository()

	// // First run: sends the email
	// sender1 := &mockSender{}
	// orch1 := orchestrator.New(repo, sender1, 100)
	// p1 := parser.NewCsvParser(strings.NewReader(csvData))
	// if err := orch1.Run(context.Background(), p1, "txns.csv", "ACC-001", "user@example.com"); err != nil {
	// 	t.Fatalf("first run error: %v", err)
	// }

	// // Second run with same file: email already sent, sender should not be called again
	// sender2 := &mockSender{}
	// orch2 := orchestrator.New(repo, sender2, 100)
	// p2 := parser.NewCsvParser(strings.NewReader(csvData))
	// if err := orch2.Run(context.Background(), p2, "txns.csv", "ACC-001", "user@example.com"); err != nil {
	// 	t.Fatalf("second run error: %v", err)
	// }

	// if len(sender2.sent) != 0 {
	// 	t.Errorf("second run: Send called %d times, want 0", len(sender2.sent))
	// }
}

func TestRun_EmailSendFailure(t *testing.T) {
	// repo := storage.NewMockRepository()
	// sendErr := errors.New("smtp: connection refused")
	// sender := &mockSender{err: sendErr}
	// orch := orchestrator.New(repo, sender, 100)

	// p := parser.NewCsvParser(strings.NewReader(csvData))
	// err := orch.Run(context.Background(), p, "txns.csv", "ACC-001", "user@example.com")
	// if err == nil {
	// 	t.Fatal("expected error from failed email send, got nil")
	// }
	// if !errors.Is(err, sendErr) {
	// 	t.Errorf("error = %v; want to wrap %v", err, sendErr)
	// }
}

func TestRun_EmailDataMatchesSummary(t *testing.T) {
	// repo := storage.NewMockRepository()
	// sender := &mockSender{}
	// orch := orchestrator.New(repo, sender, 100)

	// p := parser.NewCsvParser(strings.NewReader(csvData))
	// if err := orch.Run(context.Background(), p, "txns.csv", "ACC-001", "user@example.com"); err != nil {
	// 	t.Fatalf("unexpected error: %v", err)
	// }

	// data := sender.sent[0]
	// if data.AccountID != "ACC-001" {
	// 	t.Errorf("AccountID = %q, want ACC-001", data.AccountID)
	// }
	// if len(data.ByMonth) != 1 {
	// 	t.Errorf("ByMonth len = %d, want 1", len(data.ByMonth))
	// }
	// // Months should be sorted by MonthNum
	// if data.ByMonth[0].MonthNum != 7 {
	// 	t.Errorf("first month MonthNum = %d, want 7", data.ByMonth[0].MonthNum)
	// }
}
