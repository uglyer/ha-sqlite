package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
)

type HaSqliteStmt struct {
	driver.Stmt
	query  string
	dbId   uint64
	client proto.DBClient
}

// NewHaSqliteStmt TODO 实现真实的预编译动作
func NewHaSqliteStmt(ctx context.Context, client proto.DBClient, dbId uint64, query string) (*HaSqliteStmt, error) {
	return &HaSqliteStmt{
		query:  query,
		dbId:   dbId,
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
	return nil
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
	return -1
}

// Exec executes a query that doesn't return rows, such
// as an INSERT or UPDATE.
//
// Deprecated: Drivers should implement StmtExecContext instead (or additionally).
func (s *HaSqliteStmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), proto.ValuesToNamedValues(args))
}

// ExecContext is an optional interface that may be implemented by a Conn.
func (s *HaSqliteStmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	parameters, err := proto.DriverNamedValueToParameters(args)
	if err != nil {
		return nil, fmt.Errorf("convert named value to parameters error %v", err)
	}

	statements := []*proto.Statement{{Sql: s.query, Parameters: parameters}}
	execRequest := &proto.ExecRequest{
		Request: &proto.Request{
			DbId:       s.dbId,
			Statements: statements,
		},
	}
	resp, err := s.client.Exec(ctx, execRequest)
	if err != nil {
		return nil, fmt.Errorf("exec error: %v", err)
	}
	if resp == nil || len(resp.Result) == 0 {
		return nil, fmt.Errorf("exec without resp")
	}
	if resp.Result[0].Error != "" {
		return nil, fmt.Errorf("exec error:%s", resp.Result[0].Error)
	}
	return &execResult{
		rowsAffected: resp.Result[0].RowsAffected,
		lastInsertId: resp.Result[0].LastInsertId,
	}, nil
}

// Query executes a query that may return rows, such as a
// SELECT.
//
// Deprecated: Drivers should implement StmtQueryContext instead (or additionally).
func (s *HaSqliteStmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), proto.ValuesToNamedValues(args))
}

// QueryContext is an optional interface that may be implemented by a Conn.
func (s *HaSqliteStmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	parameters, err := proto.DriverNamedValueToParameters(args)
	if err != nil {
		return nil, fmt.Errorf("convert named value to parameters error %v", err)
	}
	statements := []*proto.Statement{{Sql: s.query, Parameters: parameters}}
	resp, err := s.client.Query(ctx, &proto.QueryRequest{Request: &proto.Request{
		DbId:       s.dbId,
		Statements: statements,
	}})
	if err != nil {
		return nil, fmt.Errorf("exec error: %v", err)
	}
	if resp == nil || len(resp.Result) == 0 {
		return nil, fmt.Errorf("exec without resp")
	}
	if resp.Result[0].Error != "" {
		return nil, fmt.Errorf("exec error:%s", resp.Result[0].Error)
	}
	return NewHaSqliteRowsFromSingleQueryResult(resp.Result[0]), nil
}
