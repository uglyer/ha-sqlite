package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"strings"
	"time"
	// Allow dialing multiple nodes with multi:///.
	_ "github.com/Jille/grpc-multi-resolver"
	// Register health checker with gRPC.
	_ "google.golang.org/grpc/health"
)

type HaSqliteConn struct {
	driver.Conn
	// Address 数据库链接地址
	Address string
	// conn 数据库连接对象
	conn *grpc.ClientConn
	// 打开成功后返回的数据库id
	dbId   int64
	Client proto.DBClient
	// 事务
	txToken string
}

const MaxTupleParams = 255
const haCreateDBSql1 = "HA CREATE DB ?"
const haCreateDBSql2 = "HA CREATE DB ?;"
const haUseSql = "HA USE ?;"
const haSnapshotSql = "HA SNAPSHOT ? TO ?;"
const haUseSqlLen = len(haUseSql)

func NewHaSqliteConn(ctx context.Context, dsn string) (*HaSqliteConn, error) {
	var o grpc.DialOption = grpc.EmptyDialOption{}
	index := -1
	if strings.HasPrefix(dsn, "multi:///") {
		l := len("multi:///")
		index = strings.Index(dsn[l:], "/") + l
	} else {
		index = strings.LastIndex(dsn, "/")
	}
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
		if dbDSN == "" {
			_, err := client.Ping(ctx, &proto.PingRequest{Timestamp: time.Now().UnixMilli()})
			if err != nil {
				errCh <- err
				return
			}
			done <- &proto.OpenResponse{DbId: 0}
		} else {
			err := checkDbName(dbDSN)
			if err != nil {
				errCh <- err
				return
			}
			resp, err := client.Open(ctx, &proto.OpenRequest{Dsn: dbDSN})
			if err != nil {
				errCh <- err
				return
			}
			done <- resp
		}
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
		Request: &proto.Request{
			DbId: c.dbId,
		},
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
		Type: finishType,
		Request: &proto.Request{
			DbId:    tx.dbId,
			TxToken: tx.txToken,
		},
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
	res, err, ok := c.parseHAExecSql(ctx, query, args)
	if ok {
		return res, err
	}
	if c.dbId == 0 {
		return nil, fmt.Errorf("exec db id is zero")
	}
	return c.ExecContextWithDbName(ctx, "", query, args)
}

// ExecContextWithDbName 通过外部的数据库名称执行sql语句
func (c *HaSqliteConn) ExecContextWithDbName(ctx context.Context, dbName string, query string, args []driver.NamedValue) (driver.Result, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	parameters, err := proto.DriverNamedValueToParameters(args)
	if err != nil {
		return nil, fmt.Errorf("convert named value to parameters error %v", err)
	}
	statements := []*proto.Statement{{Sql: query, Parameters: parameters}}
	req := &proto.Request{
		TxToken:    c.txToken,
		Statements: statements,
	}
	if dbName == "" {
		req.DbId = c.dbId
	} else {
		req.Dsn = dbName
	}
	execReq := &proto.ExecRequest{Request: req}
	return proto.DBClientExecCheckResult(c.Client, ctx, execReq)
}

// Query is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.QueryContext(context.Background(), query, proto.ValuesToNamedValues(args))
}

// QueryContext is an optional interface that may be implemented by a Conn.
func (c *HaSqliteConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	row, err, ok := c.parseHAQuerySql(ctx, query, args)
	if ok {
		return row, err
	}
	if c.dbId == 0 {
		return nil, fmt.Errorf("query db id is zero")
	}
	return c.QueryContextWithDbName(ctx, "", query, args)
}

// QueryContextWithDbName 通过外部的数据库名称执行查询sql语句
func (c *HaSqliteConn) QueryContextWithDbName(ctx context.Context, dbName string, query string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	parameters, err := proto.DriverNamedValueToParameters(args)
	if err != nil {
		return nil, fmt.Errorf("convert named value to parameters error %v", err)
	}
	statements := []*proto.Statement{{Sql: query, Parameters: parameters}}
	req := &proto.Request{
		TxToken:    c.txToken,
		Statements: statements,
	}
	if dbName == "" {
		req.DbId = c.dbId
	} else {
		req.Dsn = dbName
	}
	queryReq := &proto.QueryRequest{Request: req}
	return proto.DBClientQueryCheckResult(c.Client, ctx, queryReq)
}

// parseHAExecSql 解析 以 HA 开头的私有 sql 指令
func (c *HaSqliteConn) parseHAExecSql(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error, bool) {
	if !strings.HasPrefix(query, "HA ") {
		return nil, nil, false
	}
	if strings.HasPrefix(query, haUseSql) {
		if len(args) == 0 {
			return nil, fmt.Errorf("exec `HA USE ?;` sql only need one string db name#0, but got %v", args), true
		}
		if dbName, ok := args[0].Value.(string); ok {
			rows, err := c.ExecContextWithDbName(ctx, dbName, query[haUseSqlLen:], args[1:])
			return rows, err, true
		}
		return nil, fmt.Errorf("exec `HA USE ?;` sql only need one string arg#1, but got %v", args), true
	}
	return nil, nil, false
}

// parseHAQuerySql 解析 以 HA 开头的私有 sql 指令
func (c *HaSqliteConn) parseHAQuerySql(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error, bool) {
	if !strings.HasPrefix(query, "HA ") {
		return nil, nil, false
	}
	if query == haCreateDBSql1 || query == haCreateDBSql2 {
		if len(args) != 1 {
			return nil, fmt.Errorf("HA CREATE DB ? sql only allow one string arg#0, but got %v", args), true
		}
		if dbName, ok := args[0].Value.(string); ok {
			rows, err := c.runHAQuerySqlCreateDB(ctx, dbName)
			return rows, err, true
		}
		return nil, fmt.Errorf("HA CREATE DB ? sql only allow one string arg#1, but got %v", args), true
	}

	if strings.HasPrefix(query, haUseSql) {
		if len(args) == 0 {
			return nil, fmt.Errorf("query `HA USE ?;` sql only need one string db name#0, but got %v", args), true
		}
		if dbName, ok := args[0].Value.(string); ok {
			rows, err := c.QueryContextWithDbName(ctx, dbName, query[haUseSqlLen:], args[1:])
			return rows, err, true
		}
		return nil, fmt.Errorf("query `HA USE ?;` sql only need one string arg#1, but got %v", args), true
	}

	if strings.HasPrefix(query, haSnapshotSql) {
		if len(args) != 2 {
			return nil, fmt.Errorf("query `HA SNAPSHOT ? TO ?;` sql need db name and s3 key, but got %v", args), true
		}
		dbName, ok := args[0].Value.(string)
		if !ok {
			return nil, fmt.Errorf("query `HA SNAPSHOT ? TO ?;` parse db name error %v", args), true
		}
		s3Key, ok := args[1].Value.(string)
		if !ok {
			return nil, fmt.Errorf("query `HA SNAPSHOT ? TO ?;` parse s3 key error %v", args), true
		}
		rows, err := c.runHAQuerySqlSnapshotDB(ctx, dbName, s3Key)
		return rows, err, true
	}
	return nil, nil, false
}

// runHAQuerySqlCreateDB 执行 以 HA 开头的私有 sql 指令（创建库）
func (c *HaSqliteConn) runHAQuerySqlCreateDB(ctx context.Context, dbName string) (driver.Rows, error) {
	err := checkDbName(dbName)
	if err != nil {
		return nil, err
	}
	resp, err := c.Client.Open(ctx, &proto.OpenRequest{Dsn: dbName})
	if err != nil {
		return nil, err
	}
	result := &proto.QueryResult{
		Columns: []string{"dbId"},
		Types:   []string{"BIGINT"},
		Values: []*proto.QueryResult_Values{
			{
				Parameters: []*proto.Parameter{
					{
						Value: &proto.Parameter_I{
							I: resp.GetDbId(),
						},
					},
				},
			},
		},
	}
	return proto.NewHaSqliteRowsFromSingleQueryResult(result), nil
}

// runHAQuerySqlSnapshotDB 执行 以 HA 开头的私有 sql 指令（快照）
func (c *HaSqliteConn) runHAQuerySqlSnapshotDB(ctx context.Context, dbName string, s3Key string) (driver.Rows, error) {
	err := checkDbName(dbName)
	if err != nil {
		return nil, err
	}
	resp, err := c.Client.Snapshot(ctx, &proto.SnapshotRequest{
		Request:    &proto.Request{Dsn: dbName},
		RemotePath: s3Key,
	})
	if err != nil {
		return nil, err
	}
	result := &proto.QueryResult{
		Columns: []string{"size"},
		Types:   []string{"BIGINT"},
		Values: []*proto.QueryResult_Values{
			{
				Parameters: []*proto.Parameter{
					{
						Value: &proto.Parameter_I{
							I: resp.GetSize(),
						},
					},
				},
			},
		},
	}
	return proto.NewHaSqliteRowsFromSingleQueryResult(result), nil
}

// checkDbName 校验库名是否合法
func checkDbName(name string) error {
	if strings.Contains(name, " ") {
		return fmt.Errorf("db name dont allow `Space`")
	}
	if strings.Contains(name, "\"") ||
		strings.Contains(name, "*") ||
		strings.Contains(name, "`") ||
		strings.Contains(name, "<") ||
		strings.Contains(name, ">") ||
		strings.Contains(name, "?") ||
		strings.Contains(name, "\\") ||
		strings.Contains(name, "|") ||
		strings.Contains(name, ":") {
		return fmt.Errorf("db name dont allow \"（双引号）、*（星号）、<（小于）、>（大于）、?（问号）、\\（反斜杠）、|（竖线）、/ (正斜杠)、 : (冒号)`")
	}
	return nil
}
