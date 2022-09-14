package main

import (
	"database/sql"
	"fmt"
)

// QueryRows represents query result
type QueryRows struct {
	rowCount int
	colCount int
	Columns  []string
	Types    []string
	Values   [][]interface{}
}

func parseSqlRows(rows *sql.Rows) (*QueryRows, error) {
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("parseSqlRows get columns error:%v\n", err)
	}
	colCount := len(cols)
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("parseSqlRows get columnTypes error:%v\n", err)
	}
	types := make([]string, colCount)
	for i, colType := range columnTypes {
		types[i] = colType.DatabaseTypeName()
	}

	var values [][]interface{}
	for rows.Next() {
		row := make([]interface{}, colCount)
		err := rows.Scan(row...)
		if err != nil {
			return nil, fmt.Errorf("parseSqlRows rows.Scan error:%v\n", err)
		}
		values = append(values, row)
	}
	return &QueryRows{
		Columns:  cols,
		Types:    types,
		Values:   values,
		rowCount: len(values) + 1,
		colCount: colCount,
	}, nil
}

// RowCount implements textutil.Table interface
func (r *QueryRows) RowCount() int {
	return r.rowCount
}

// ColCount implements textutil.Table interface
func (r *QueryRows) ColCount() int {
	return r.colCount
}

// Get implements textutil.Table interface
func (r *QueryRows) Get(i, j int) string {
	if i == 0 {
		if j >= len(r.Columns) {
			return ""
		}
		return r.Columns[j]
	}

	if r.Values == nil {
		return "NULL"
	}

	if i-1 >= len(r.Values) {
		return "NULL"
	}
	if j >= len(r.Values[i-1]) {
		return "NULL"
	}
	return fmt.Sprintf("%v", r.Values[i-1][j])
}
