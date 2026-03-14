// Package parser provides a generic, business-logic-agnostic CSV reader.
// It streams rows one at a time and maps header columns to struct fields
// using the `csv` struct tag (or exact field name as fallback).
package parser

import (
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
)

// Parser is the contract for reading structured data row by row.
type Parser interface {
	// ReadHeader reads the first row and stores the column names.
	// Must be called before the first Scan.
	ReadHeader() error

	// Scan reads the next row and populates dest using the stored headers.
	// dest must be a non-nil pointer to a struct.
	// Returns io.EOF when there are no more rows.
	Scan(dest any) error
}

// CsvParser is a stream-based CSV parser that maps columns to struct fields
// via the `csv:"column_name"` struct tag (falls back to lowercase field name).
type CsvParser struct {
	reader  *csv.Reader
	headers []string // column names from the first row
}

// NewCsvParser creates a CsvParser from any io.Reader (file, S3 stream, etc.).
func NewCsvParser(r io.Reader) *CsvParser {
	return &CsvParser{
		reader: csv.NewReader(r),
	}
}

// ReadHeader reads and stores the CSV header row.
func (p *CsvParser) ReadHeader() error {
	headers, err := p.reader.Read()
	if err != nil {
		return fmt.Errorf("parser: reading header: %w", err)
	}
	// Normalize to lowercase to make matching case-insensitive.
	for i, h := range headers {
		headers[i] = strings.ToLower(strings.TrimSpace(h))
	}
	p.headers = headers
	return nil
}

// Scan reads the next CSV row and fills dest using the stored headers.
// dest must be a pointer to a struct. Struct fields are matched by the
// `csv:"name"` tag, or by lowercase field name if the tag is absent.
// Returns io.EOF when all rows have been consumed.
func (p *CsvParser) Scan(dest any) error {
	if p.headers == nil {
		return fmt.Errorf("parser: ReadHeader must be called before Scan")
	}

	record, err := p.reader.Read()
	if err != nil {
		// io.EOF is returned as-is so callers can detect end of file.
		return err
	}

	return mapRecord(record, p.headers, dest)
}

// mapRecord fills dest struct fields from a CSV record using header names.
func mapRecord(record, headers []string, dest any) error {
	rv := reflect.ValueOf(dest)
	if rv.Kind() != reflect.Ptr || rv.IsNil() {
		return fmt.Errorf("parser: dest must be a non-nil pointer to a struct")
	}
	rv = rv.Elem()
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("parser: dest must point to a struct, got %s", rv.Kind())
	}

	rt := rv.Type()

	// Build a map: csv column name → struct field index for O(1) lookup.
	fieldIndex := make(map[string]int, rt.NumField())
	for i := 0; i < rt.NumField(); i++ {
		f := rt.Field(i)
		tag := f.Tag.Get("csv")
		if tag == "" || tag == "-" {
			if tag == "-" {
				continue
			}
			fieldIndex[strings.ToLower(f.Name)] = i
		} else {
			fieldIndex[strings.ToLower(tag)] = i
		}
	}

	for col, raw := range record {
		if col >= len(headers) {
			break
		}
		name := headers[col]
		idx, ok := fieldIndex[name]
		if !ok {
			continue // unknown column — skip silently
		}

		fv := rv.Field(idx)
		if err := setField(fv, raw); err != nil {
			return fmt.Errorf("parser: field %q (column %q): %w", rt.Field(idx).Name, name, err)
		}
	}
	return nil
}

// setField sets a reflect.Value from a raw string, handling common Go types.
func setField(fv reflect.Value, raw string) error {
	raw = strings.TrimSpace(raw)
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("expected int, got %q", raw)
		}
		fv.SetInt(n)
	case reflect.Float32, reflect.Float64:
		// Strip leading + so strconv.ParseFloat accepts "+60.5"
		f, err := strconv.ParseFloat(strings.TrimPrefix(raw, "+"), 64)
		if err != nil {
			return fmt.Errorf("expected float, got %q", raw)
		}
		fv.SetFloat(f)
	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("expected bool, got %q", raw)
		}
		fv.SetBool(b)
	default:
		return fmt.Errorf("unsupported field type %s", fv.Kind())
	}
	return nil
}
