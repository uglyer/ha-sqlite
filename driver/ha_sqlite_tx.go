package driver

import (
	"context"
	"database/sql/driver"
	"fmt"
)

type HaSqliteTx struct {
	driver.Tx
}

func NewHaSqliteTx(ctx context.Context, opts driver.TxOptions) (*HaSqliteTx, error) {
	return &HaSqliteTx{}, nil
}

func (tx *HaSqliteTx) Commit() error {
	return fmt.Errorf("todo impl HaSqliteTx Commit")
}

func (tx *HaSqliteTx) Rollback() error {
	return fmt.Errorf("todo impl HaSqliteTx Rollback")
}
