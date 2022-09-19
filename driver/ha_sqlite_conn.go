package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"time"
	// Allow dialing multiple nodes with multi:///.
	_ "github.com/Jille/grpc-multi-resolver"
	// Register health checker with gRPC.
	_ "google.golang.org/grpc/health"
	"strings"
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
	// 事务
	txToken string
}

const MaxTupleParams = 255

func NewHaSqliteConn(ctx context.Context, dsn string) (*HaSqliteConn, error) {
	var o grpc.DialOption = grpc.EmptyDialOption{}
	index := strings.LastIndex(dsn, "/")
	if index < 0 {
		return nil, fmt.Errorf("NewHaSqliteConn error dsn: %v, example: \"localhost:30051/db-name.db\"", dsn)
	}
	address := dsn[:index]
	dbDSN := dsn[index+1:]
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), o)
	if err != nil {
		return nil, fmt.Errorf("NewHaSqliteConn open conn error#1: %v", err)
	}
	client := proto.NewDBClient(conn)
	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*4000)
	defer cancel()
	done := make(chan *proto.OpenResponse, 1)
	errCh := make(chan error, 1)
	defer close(done)
	defer close(errCh)
	go func() {
		resp, err := client.Open(ctx, &proto.OpenRequest{Dsn: dbDSN})
		if err != nil {
			errCh <- err
		}
		done <- resp
	}()
	select {
	case err := <-errCh:
		return nil, err
	case resp := <-done:
		haConn := &HaSqliteConn{
			Address: address,
			conn:    conn,
			Client:  client,
			dbId:    resp.DbId,
		}
		return haConn, nil
	case <-time.After(5000 * time.Millisecond):
		fmt.Println("timeout!!!")
		return nil, fmt.Errorf("connect time dsn:%s", dsn)
	}
}

// Ping is an optional interface that may be implemented by a Conn.
//
// If a Conn does not implement Pinger, the sql package's DB.Ping and
// DB.PingContext will check if there is at least one Conn available.
//
// If Conn.Ping returns ErrBadConn, DB.Ping and DB.PingContext will remove
// the Conn from pool.}
func (c *HaSqliteConn) Ping(ctx context.Context) error {
	req := &proto.PingRequest{
		Timestamp: time.Now().UnixMilli(),
	}
	_, err := c.Client.Ping(context.Background(), req)
	return err
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
	return NewHaSqliteStmt(context.Background(), c.Client, c.dbId, c.txToken, query)
}

// PrepareContext returns a prepared statement, bound to this connection.
// context is for the preparation of the statement, it must not store the
// context within the statement itself.
func (c *HaSqliteConn) PrepareContext(ctx context.Context, query string) (driver.StmtExecContext, error) {
	return NewHaSqliteStmt(ctx, c.Client, c.dbId, c.txToken, query)
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *HaSqliteConn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
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
	resp, err := c.Client.BeginTx(ctx, &proto.BeginTxRequest{
		Type: proto.BeginTxRequest_TX_TYPE_BEGIN_LevelLinearizable,
		DbId: c.dbId,
	})
	if err != nil {
		return nil, fmt.Errorf("BeginTx error: %v", err)
	}
	c.txToken = resp.TxToken
	return NewHaSqliteTx(c)
}

func (tx *HaSqliteConn) finishTx(finishType proto.FinishTxRequest_Type) error {
	defer func() {
		tx.txToken = ""
	}()
	req := &proto.FinishTxRequest{
		Type:    finishType,
		TxToken: tx.txToken,
		DbId:    tx.dbId,
	}
	_, err := tx.Client.FinishTx(context.Background(), req)
	return err
}

// Exec is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) Exec(query string, args []driver.Value) (driver.Result, error) {
	return c.ExecContext(context.Background(), query, proto.ValuesToNamedValues(args))
}

// ExecContext is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if c.dbId == 0 {
		return nil, fmt.Errorf("exec db id is zero")
	}
	return c.ExecContextWithDbId(ctx, c.dbId, query, args)
}

// ExecContextWithDbId 通过外部的数据库id执行sql语句
func (c *HaSqliteConn) ExecContextWithDbId(ctx context.Context, dbId uint64, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	parameters, err := proto.DriverNamedValueToParameters(args)
	if err != nil {
		return nil, fmt.Errorf("convert named value to parameters error %v", err)
	}
	statements := []*proto.Statement{{Sql: query, Parameters: parameters}}
	req := &proto.ExecRequest{Request: &proto.Request{
		TxToken:    c.txToken,
		DbId:       dbId,
		Statements: statements,
	}}
	return proto.DBClientExecCheckResult(c.Client, ctx, req)
}

// Query is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, proto.ValuesToNamedValues(args))
}

// QueryContext is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.dbId == 0 {
		return nil, fmt.Errorf("query db id is zero")
	}
	return c.QueryContextWithDbId(ctx, c.dbId, query, args)
}

// QueryContextWithDbId 通过外部的数据库id查询sql语句
func (c *HaSqliteConn) QueryContextWithDbId(ctx context.Context, dbId uint64, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	parameters, err := proto.DriverNamedValueToParameters(args)
	if err != nil {
		return nil, fmt.Errorf("convert named value to parameters error %v", err)
	}
	statements := []*proto.Statement{{Sql: query, Parameters: parameters}}
	req := &proto.QueryRequest{Request: &proto.Request{
		TxToken:    c.txToken,
		DbId:       dbId,
		Statements: statements,
	}}
	return proto.DBClientQueryCheckResult(c.Client, ctx, req)
}
