package aggregator_test

import (
	"context"
	"math"
	"strings"
	"testing"

	"github.com/luisDiazStgo1994/txn-processor/internal/aggregator"
	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
	"github.com/luisDiazStgo1994/txn-processor/internal/storage"
)

// csvWith is a helper that prepends the header to a CSV body.
func csvWith(rows string) string {
	return "id,date,transaction\n" + rows
}

// newAgg builds an Aggregator backed by a fresh MockRepository.
func newAgg(csv, fileKey string) (*aggregator.Aggregator, *storage.MockRepository) {
	repo := storage.NewMockRepository()
	p := parser.NewCsvParser(strings.NewReader(csv))
	agg := aggregator.New(p, repo, "ACC-001", fileKey, 100)
	return agg, repo
}

func approxEqual(a, b float64) bool {
	return math.Abs(a-b) < 1e-9
}

func TestCompute_TotalBalance(t *testing.T) {
	csv := csvWith("0,7/15,+60.5\n1,7/28,-10.3\n2,8/2,-20.46\n")
	agg, _ := newAgg(csv, "key1")

	summary, err := agg.Compute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := 60.5 + (-10.3) + (-20.46)
	if !approxEqual(summary.TotalBalance, want) {
		t.Errorf("TotalBalance = %v, want %v", summary.TotalBalance, want)
	}
}

func TestCompute_ByMonthAggregates(t *testing.T) {
	csv := csvWith("0,7/15,+60.5\n1,7/28,-10.3\n2,8/2,-20.46\n")
	agg, _ := newAgg(csv, "key2")

	summary, err := agg.Compute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	july, ok := summary.ByMonth["July"]
	if !ok {
		t.Fatal("missing July in ByMonth")
	}
	if july.TxnCount != 2 {
		t.Errorf("July TxnCount = %d, want 2", july.TxnCount)
	}
	if july.AvgCredit != 60.5 {
		t.Errorf("July AvgCredit = %v, want 60.5", july.AvgCredit)
	}
	if july.AvgDebit != -10.3 {
		t.Errorf("July AvgDebit = %v, want -10.3", july.AvgDebit)
	}
	if july.MonthNum != 7 {
		t.Errorf("July MonthNum = %d, want 7", july.MonthNum)
	}

	aug, ok := summary.ByMonth["August"]
	if !ok {
		t.Fatal("missing August in ByMonth")
	}
	if aug.TxnCount != 1 {
		t.Errorf("August TxnCount = %d, want 1", aug.TxnCount)
	}
	if aug.AvgDebit != -20.46 {
		t.Errorf("August AvgDebit = %v, want -20.46", aug.AvgDebit)
	}
}

func TestCompute_MalformedRowSkipped(t *testing.T) {
	// row 1 has a bad date — should be skipped without aborting the run
	csv := csvWith("0,7/15,+60.5\n1,baddate,-10.3\n2,8/2,-20.46\n")
	agg, _ := newAgg(csv, "key3")

	summary, err := agg.Compute(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := 60.5 + (-20.46) // row 1 skipped
	if !approxEqual(summary.TotalBalance, want) {
		t.Errorf("TotalBalance = %v, want %v", summary.TotalBalance, want)
	}
	if len(summary.ByMonth) != 2 {
		t.Errorf("ByMonth len = %d, want 2", len(summary.ByMonth))
	}
}

func TestCompute_FileProcessingStatusDone(t *testing.T) {
	csv := csvWith("0,7/15,+60.5\n")
	agg, repo := newAgg(csv, "key4")

	if _, err := agg.Compute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fp, err := repo.GetFileProcessing(context.Background(), "key4")
	if err != nil {
		t.Fatalf("get file processing: %v", err)
	}
	if fp.Status != storage.FileStatusDone {
		t.Errorf("Status = %q, want %q", fp.Status, storage.FileStatusDone)
	}
}

func TestCompute_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already done before Compute is called

	csv := csvWith("0,7/15,+60.5\n1,7/28,-10.3\n")
	agg, repo := newAgg(csv, "key5")

	_, err := agg.Compute(ctx)
	if err == nil {
		t.Fatal("expected error on cancelled context, got nil")
	}

	fp, _ := repo.GetFileProcessing(context.Background(), "key5")
	if fp.Status != storage.FileStatusFailed {
		t.Errorf("Status = %q, want %q", fp.Status, storage.FileStatusFailed)
	}
}

func TestCompute_CheckpointFlushedMidFile(t *testing.T) {
	// checkpointInterval=2, 4 rows → checkpoint should be flushed after row 2
	csv := csvWith("0,7/15,+10\n1,7/16,+20\n2,7/17,+30\n3,7/18,+40\n")
	repo := storage.NewMockRepository()
	p := parser.NewCsvParser(strings.NewReader(csv))
	agg := aggregator.New(p, repo, "ACC-001", "key6", 2)

	if _, err := agg.Compute(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	fp, err := repo.GetFileProcessing(context.Background(), "key6")
	if err != nil {
		t.Fatalf("get file processing: %v", err)
	}
	// Final checkpoint = txnCount = 4
	if fp.CheckpointRow != 4 {
		t.Errorf("CheckpointRow = %d, want 4", fp.CheckpointRow)
	}
}
