package driver

import (
	"database/sql/driver"
)

type HaSqliteDriver struct {
	driver.Driver
	DriverName string
}

func NewHaSqliteDriver() *HaSqliteDriver {
	return &HaSqliteDriver{
		DriverName: "ha-sqlite",
	}
}

// Open 支持 multi:/// 或单个链接地址
func (d *HaSqliteDriver) Open(name string) (driver.Conn, error) {
	return NewHaSqliteConn(name)
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *HaSqliteDriver) OpenConnector(name string) (driver.Connector, error) {
	return &HaSqliteConnector{
		address: name,
		drive:   d,
	}, nil
}
