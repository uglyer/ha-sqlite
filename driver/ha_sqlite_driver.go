package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

type HaSqliteDriver struct {
	driver.Driver
	DriverName string
}

// 注册驱动
func init() {
	d := NewHaSqliteDriver()
	sql.Register(d.DriverName, d)
}

func NewHaSqliteDriver() *HaSqliteDriver {
	return &HaSqliteDriver{
		DriverName: "ha-sqlite",
	}
}

// Open 支持 multi:/// 或单个链接地址
func (d *HaSqliteDriver) Open(name string) (driver.Conn, error) {
	connector, err := d.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector must parse the name in the same format that Driver.Open
// parses the name parameter.
func (d *HaSqliteDriver) OpenConnector(name string) (driver.Connector, error) {
	return &HaSqliteConnector{
		address: name,
		drive:   d,
	}, nil
}
