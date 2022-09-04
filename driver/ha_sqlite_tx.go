package driver

import (
	"database/sql/driver"
	"github.com/uglyer/ha-sqlite/proto"
)

type HaSqliteTx struct {
	driver.Tx
	dbId uint64
	conn *HaSqliteConn
}

func NewHaSqliteTx(conn *HaSqliteConn) (*HaSqliteTx, error) {
	return &HaSqliteTx{
		conn: conn,
	}, nil
}

func (tx *HaSqliteTx) Commit() error {
	return tx.conn.finishTx(proto.FinishTxRequest_TX_TYPE_COMMIT)
}

func (tx *HaSqliteTx) Rollback() error {
	return tx.conn.finishTx(proto.FinishTxRequest_TX_TYPE_ROLLBACK)
}
