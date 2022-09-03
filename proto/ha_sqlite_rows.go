package proto

import (
	"database/sql/driver"
	"io"
)

type HaSqliteRows struct {
	r     *QueryResult
	index int
	len   int
}

func NewHaSqliteRowsFromSingleQueryResult(r *QueryResult) *HaSqliteRows {
	return &HaSqliteRows{
		r:     r,
		index: 0,
		len:   len(r.Values),
	}
}

// Columns returns the names of the columns. The number of
// columns of the result is inferred from the length of the
// slice. If a particular column name isn't known, an empty
// string should be returned for that entry.
func (r *HaSqliteRows) Columns() []string {
	return r.r.Columns
}

// Close closes the rows iterator.
func (r *HaSqliteRows) Close() error {
	return nil
}

// Next is called to populate the next row of data into
// the provided slice. The provided slice will be the same
// size as the Columns() are wide.
//
// Next should return io.EOF when there are no more rows.
//
// The dest should not be written to outside of Next. Care
// should be taken when closing Rows not to modify
// a buffer held in dest.
func (r *HaSqliteRows) Next(dest []driver.Value) error {
	if r.index >= r.len {
		return io.EOF
	}
	err := ParametersCopyToDriverValues(dest, r.r.Values[r.index].Parameters)
	if err != nil {
		return err
	}
	r.index++
	return nil
}
