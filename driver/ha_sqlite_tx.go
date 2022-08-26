package driver

import (
	"database/sql/driver"
	"fmt"
)

type HaSqliteTx struct {
	driver.Tx
}

func NewHaSqliteTx() (*HaSqliteTx, error) {
	return &HaSqliteTx{}, nil
}

func (tx *HaSqliteTx) Commit() error {
	return fmt.Errorf("todo impl HaSqliteTx Commit")
}

func (tx *HaSqliteTx) Rollback() error {
	return fmt.Errorf("todo impl HaSqliteTx Rollback")
}
