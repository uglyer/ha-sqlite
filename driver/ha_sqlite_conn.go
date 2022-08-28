package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
)

type HaSqliteConn struct {
	driver.Conn
	// Address 数据库链接地址
	Address string
	// conn 数据库连接对象
	conn *grpc.ClientConn
	// 打开成功后返回的数据库id
	dbId   uint64
	Client proto.DBClient
	ctx    context.Context
}

const MaxTupleParams = 255

func NewHaSqliteConn(ctx context.Context, address string) (*HaSqliteConn, error) {
	// todo 超时时间等参数处理
	var o grpc.DialOption = grpc.EmptyDialOption{}
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), o)
	if err != nil {
		return nil, fmt.Errorf("NewHaSqliteConn open conn error#1: %v", err)
	}
	client := proto.NewDBClient(conn)
	resp, err := client.Open(ctx, &proto.OpenRequest{Dsn: address})
	if err != nil {
		return nil, fmt.Errorf("NewHaSqliteConn open conn error#2: %v", err)
	}
	return &HaSqliteConn{
		Address: address,
		conn:    conn,
		dbId:    resp.DbId,
		Client:  client,
	}, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use.
//
// Because the sql package maintains a free pool of
// connections and only calls Close when there's a surplus of
// idle connections, it shouldn't be necessary for drivers to
// do their own connection caching.
//
// Drivers must ensure all network calls made by Close
// do not block indefinitely (e.g. apply a timeout).
func (c *HaSqliteConn) Close() error {
	return c.conn.Close()
}

// Prepare returns a prepared statement, bound to this connection.
func (c *HaSqliteConn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement, it must not store the
// context within the statement itself.
func (c *HaSqliteConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return NewHaSqliteStmt(ctx, c.Client, query)
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *HaSqliteConn) Begin() (driver.Tx, error) {
	return NewHaSqliteTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction.  If the context is canceled by
// the user the sql package will call Tx.Rollback before discarding and closing
// the connection.
//
// This must check opts.Isolation to determine if there is a set isolation
// level. If the driver does not support a non-default level and one is set or
// if there is a non-default isolation level that is not supported, an error
// must be returned.
//
// This must also check opts.ReadOnly to determine if the read-only value is
// true to either set the read-only transaction property if supported or return
// an error if it is not supported.
func (c *HaSqliteConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return NewHaSqliteTx(ctx, opts)
}

// Exec is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, ValuesToNamedValues(args))
}

// ExecContext is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	//c.Client.Exec(ctx,)
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	parameters, err := DriverNamedValueToParameters(args)
	if err != nil {
		return nil, fmt.Errorf("convert named value to parameters error %v", err)
	}
	statements := make([]*proto.Statement, 1)
	statements[0] = &proto.Statement{Sql: query, Parameters: parameters}
	resp, err := c.Client.Exec(ctx, &proto.ExecRequest{Request: &proto.Request{
		DbId:       c.dbId,
		Statements: statements,
	}})
	if resp == nil || len(resp.Result) == 0 {
		return nil, fmt.Errorf("todo impl ExecContext")
	}
	return &execResult{
		rowsAffected: resp.Result[0].RowsAffected,
		lastInsertId: resp.Result[0].LastInsertId,
	}, fmt.Errorf("todo impl ExecContext")
}

type execResult struct {
	rowsAffected int64
	lastInsertId int64
}

func (result *execResult) LastInsertId() (int64, error) {
	return result.lastInsertId, nil
}

func (result *execResult) RowsAffected() (int64, error) {
	return result.rowsAffected, nil
}

// Query is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, ValuesToNamedValues(args))
}

// QueryContext is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	return nil, fmt.Errorf("todo impl QueryContext")
}