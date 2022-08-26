package driver

import (
	"database/sql/driver"
	"fmt"
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
	return nil, fmt.Errorf("todo impl ha-sqlite drive open")
}
