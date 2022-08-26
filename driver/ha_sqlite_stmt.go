package driver

import (
	"database/sql/driver"
	"fmt"
)

type HaSqliteStmt struct {
	driver.Stmt
	query string
}

func NewHaSqliteStmt(query string) (*HaSqliteStmt, error) {
	return &HaSqliteStmt{
		query: query,
	}, nil
}

// Close closes the statement.
//
// As of Go 1.1, a Stmt will not be closed if it's in use
// by any queries.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (s *HaSqliteStmt) Close() error {
	return fmt.Errorf("todo impl HaSqliteStmt Close")
}

// NumInput returns the number of placeholder parameters.
//
// If NumInput returns >= 0, the sql package will sanity check
// argument counts from callers and return errors to the caller
// before the statement's Exec or Query methods are called.
//
// NumInput may also return -1, if the driver doesn't know
// its number of placeholders. In that case, the sql package
// will not sanity check Exec or Query argument counts.
func (s *HaSqliteStmt) NumInput() int {
	return 0
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *HaSqliteStmt) Exec(args []driver.Value) (driver.Result, error) {
	return nil, fmt.Errorf("todo impl HaSqliteStmt Exec")
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *HaSqliteStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, fmt.Errorf("todo impl HaSqliteStmt Query")
}
