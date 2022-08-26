package driver

import (
	"context"
	"database/sql/driver"
)

// HaSqliteConnector A Connector represents a driver in a fixed configuration
// and can create any number of equivalent Conns for use
// by multiple goroutines.
//
// A Connector can be passed to sql.OpenDB, to allow drivers
// to implement their own sql.DB constructors, or returned by
// DriverContext's OpenConnector method, to allow drivers
// access to context and to avoid repeated parsing of driver
// configuration.
//
// If a Connector implements io.Closer, the sql package's DB.Close
// method will call Close and return error (if any).
type HaSqliteConnector struct {
	driver.Connector
	drive   driver.Driver
	address string
}

// Connect returns a connection to the database.
// Connect may return a cached connection (one previously
// closed), but doing so is unnecessary; the sql package
// maintains a pool of idle connections for efficient re-use.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for
// other purposes. A default timeout should still be used
// when dialing as a connection pool may call Connect
// asynchronously to any query.
//
// The returned connection is only used by one goroutine at a
// time.
func (c *HaSqliteConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return NewHaSqliteConn(c.address)
}

// Driver returns the underlying Driver of the Connector,
// mainly to maintain compatibility with the Driver method
// on sql.DB.
func (c *HaSqliteConnector) Driver() driver.Driver {
	return c.drive
}
