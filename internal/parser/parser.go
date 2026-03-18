// Package parser provides a CSV reader for transaction files.
// It streams rows one at a time and maps them to a typed TransactionRow DTO.
package parser

import (
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

const dateLayout = "02/01/2006" // dd/mm/yyyy

// TransactionRow is the DTO produced by each Scan call.
type TransactionRow struct {
	ID     int
	Date   time.Time
	Amount float64
}

// Parser is the contract for reading transaction rows one at a time.
type Parser interface {
	// ReadHeader reads the first row and stores the column positions.
	// Must be called before the first Scan.
	ReadHeader() error

	// Scan reads the next row and populates dest.
	// Returns io.EOF when there are no more rows.
	Scan(dest *TransactionRow) error
}

// CsvParser is a stream-based CSV parser for transaction files.
type CsvParser struct {
	reader *csv.Reader
	colIdx map[string]int // column name → position in record
}

// NewCsvParser creates a CsvParser from any io.Reader (file, S3 stream, etc.).
func NewCsvParser(r io.Reader) *CsvParser {
	return &CsvParser{reader: csv.NewReader(r)}
}

// requiredColumns lists the columns that must be present in every transaction file.
var requiredColumns = []string{"id", "date", "transaction"}

// ReadHeader reads and stores the CSV header row, returning an error if any
// required column is missing. This is the single validation point for file schema.
func (p *CsvParser) ReadHeader() error {
	headers, err := p.reader.Read()
	if err != nil {
		return fmt.Errorf("parser: reading header: %w", err)
	}
	p.colIdx = make(map[string]int, len(headers))
	for i, h := range headers {
		p.colIdx[strings.ToLower(strings.TrimSpace(h))] = i
	}
	for _, col := range requiredColumns {
		if _, ok := p.colIdx[col]; !ok {
			return fmt.Errorf("parser: missing required column %q", col)
		}
	}
	return nil
}

// Scan reads the next CSV row and fills dest.
// Returns io.EOF when all rows have been consumed.
// ReadHeader guarantees all required columns exist, so lookups are direct.
func (p *CsvParser) Scan(dest *TransactionRow) error {
	if p.colIdx == nil {
		return fmt.Errorf("parser: ReadHeader must be called before Scan")
	}

	record, err := p.reader.Read()
	if err != nil {
		return err
	}

	dest.ID, err = strconv.Atoi(strings.TrimSpace(record[p.colIdx["id"]]))
	if err != nil {
		return fmt.Errorf("parser: field ID: expected int, got %q", record[p.colIdx["id"]])
	}

	dest.Date, err = time.Parse(dateLayout, strings.TrimSpace(record[p.colIdx["date"]]))
	if err != nil {
		return fmt.Errorf("parser: field Date: expected dd/mm/yyyy, got %q", record[p.colIdx["date"]])
	}

	raw := strings.TrimSpace(record[p.colIdx["transaction"]])
	dest.Amount, err = strconv.ParseFloat(strings.TrimPrefix(raw, "+"), 64)
	if err != nil {
		return fmt.Errorf("parser: field Amount: expected float, got %q", raw)
	}

	return nil
}
