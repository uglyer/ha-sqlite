package driver

import (
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
	return nil, fmt.Errorf("todo impl HaSqliteConn Begin")
}
