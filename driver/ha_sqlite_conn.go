package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/shimingyah/pool"
)

type HaSqliteConn struct {
	driver.Conn
	// Address 数据库链接地址
	Address string
	// connPool 数据库连接池
	connPool pool.Pool
}

func NewHaSqliteConn(address string) (*HaSqliteConn, error) {
	return nil, fmt.Errorf("todo impl NewHaSqliteConn")
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
	return fmt.Errorf("todo impl HaSqliteConn Close")
}

// Prepare returns a prepared statement, bound to this connection.
func (c *HaSqliteConn) Prepare(query string) (driver.Stmt, error) {
	return NewHaSqliteStmt(query)
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *HaSqliteConn) Begin() (driver.Tx, error) {
	return NewHaSqliteTx()
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
	return NewHaSqliteTx()
}
