package parser_test

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
)

const sampleCSV = `Id,Date,Transaction
15/07/2026,+60.5
1,28/07/2026,-10.3
2,02/08/2026,-20.46
3,13/08/2026,+10
`

func newParser(data string) *parser.CsvParser {
	return parser.NewCsvParser(strings.NewReader(data))
}

func TestReadHeader(t *testing.T) {
	p := newParser(sampleCSV)
	if err := p.ReadHeader(); err != nil {
		t.Fatalf("ReadHeader: %v", err)
	}
}

func TestScanAllRows(t *testing.T) {
	csv := `Id,Date,Transaction
0,15/07/2026,+60.5
1,28/07/2026,-10.3
2,02/08/2026,-20.46
3,13/08/2026,+10
`
	p := newParser(csv)
	if err := p.ReadHeader(); err != nil {
		t.Fatal(err)
	}

	type want struct {
		id     int
		date   time.Time
		amount float64
	}
	wants := []want{
		{0, time.Date(2026, 7, 15, 0, 0, 0, 0, time.UTC), +60.5},
		{1, time.Date(2026, 7, 28, 0, 0, 0, 0, time.UTC), -10.3},
		{2, time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC), -20.46},
		{3, time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC), +10},
	}

	for i, w := range wants {
		var row parser.TransactionRow
		if err := p.Scan(&row); err != nil {
			t.Fatalf("row %d: Scan: %v", i, err)
		}
		if row.ID != w.id {
			t.Errorf("row %d: ID = %d, want %d", i, row.ID, w.id)
		}
		if !row.Date.Equal(w.date) {
			t.Errorf("row %d: Date = %v, want %v", i, row.Date, w.date)
		}
		if row.Amount != w.amount {
			t.Errorf("row %d: Amount = %v, want %v", i, row.Amount, w.amount)
		}
	}

	var extra parser.TransactionRow
	if err := p.Scan(&extra); err != io.EOF {
		t.Errorf("expected io.EOF after last row, got %v", err)
	}
}

func TestScanBeforeReadHeader(t *testing.T) {
	p := newParser(sampleCSV)
	var row parser.TransactionRow
	if err := p.Scan(&row); err == nil {
		t.Error("expected error when Scan called before ReadHeader")
	}
}

func TestScanBadDate(t *testing.T) {
	csv := "id,date,transaction\n1,not-a-date,+5.0\n"
	p := newParser(csv)
	_ = p.ReadHeader()
	var row parser.TransactionRow
	if err := p.Scan(&row); err == nil {
		t.Error("expected error for invalid date format")
	}
}
