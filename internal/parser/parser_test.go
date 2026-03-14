package parser_test

import (
	"io"
	"strings"
	"testing"

	"github.com/luisDiazStgo1994/txn-processor/internal/parser"
)

const sampleCSV = `Id,Date,Transaction
0,7/15,+60.5
1,7/28,-10.3
2,8/2,-20.46
3,8/13,+10
`

type rawRow struct {
	ID          int     `csv:"id"`
	Date        string  `csv:"date"`
	Transaction float64 `csv:"transaction"`
}

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
	p := newParser(sampleCSV)
	if err := p.ReadHeader(); err != nil {
		t.Fatal(err)
	}

	want := []rawRow{
		{0, "7/15", +60.5},
		{1, "7/28", -10.3},
		{2, "8/2", -20.46},
		{3, "8/13", +10},
	}

	for i, w := range want {
		var row rawRow
		if err := p.Scan(&row); err != nil {
			t.Fatalf("row %d: Scan: %v", i, err)
		}
		if row != w {
			t.Errorf("row %d: got %+v, want %+v", i, row, w)
		}
	}

	var extra rawRow
	if err := p.Scan(&extra); err != io.EOF {
		t.Errorf("expected io.EOF after last row, got %v", err)
	}
}

func TestScanBeforeReadHeader(t *testing.T) {
	p := newParser(sampleCSV)
	var row rawRow
	err := p.Scan(&row)
	if err == nil {
		t.Error("expected error when Scan called before ReadHeader")
	}
}

func TestScanNonPointer(t *testing.T) {
	p := newParser(sampleCSV)
	_ = p.ReadHeader()
	var row rawRow
	err := p.Scan(row) // not a pointer
	if err == nil {
		t.Error("expected error for non-pointer dest")
	}
}

func TestScanUnknownColumnsIgnored(t *testing.T) {
	csv := "id,date,transaction,unknown\n1,7/1,+5.0,extra\n"
	p := newParser(csv)
	_ = p.ReadHeader()
	var row rawRow
	if err := p.Scan(&row); err != nil {
		t.Errorf("unexpected error with unknown column: %v", err)
	}
	if row.ID != 1 {
		t.Errorf("expected ID=1, got %d", row.ID)
	}
}
