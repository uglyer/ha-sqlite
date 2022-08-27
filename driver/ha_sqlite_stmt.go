package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
)

type HaSqliteStmt struct {
	driver.Stmt
	ctx    context.Context
	query  string
	client proto.DBClient
}

func NewHaSqliteStmt(ctx context.Context, client proto.DBClient, query string) (*HaSqliteStmt, error) {
	return &HaSqliteStmt{
		ctx:    ctx,
		query:  query,
		client: client,
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
	//s.client.Exec(s.ctx,)
	return s.ExecContext(s.ctx, valuesToNamedValues(args))
}

// ExecContext is an optional interface that may be implemented by a Conn.
func (s *HaSqliteStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	//c.Client.Exec(ctx,)
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	return nil, fmt.Errorf("todo impl ExecContext")
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *HaSqliteStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext is an optional interface that may be implemented by a Conn.
func (c *HaSqliteStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	return nil, fmt.Errorf("todo impl QueryContext")
}
