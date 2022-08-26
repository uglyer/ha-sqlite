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

func (c *HaSqliteConn) Close() error {
	return fmt.Errorf("todo impl HaSqliteConn Close")
}

func (c *HaSqliteConn) Prepare(query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("todo impl HaSqliteConn Prepare")
}

func (c *HaSqliteConn) Begin() (driver.Tx, error) {
	return nil, fmt.Errorf("todo impl HaSqliteConn Begin")
}
