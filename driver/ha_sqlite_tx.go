package driver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
)

type HaSqliteTx struct {
	driver.Tx
	txToken string
	dbId    uint64
	client  proto.DBClient
}

func NewHaSqliteTx(client proto.DBClient, txToken string, dbId uint64) (*HaSqliteTx, error) {
	return &HaSqliteTx{
		client:  client,
		txToken: txToken,
		dbId:    dbId,
	}, nil
}

func (tx *HaSqliteTx) Exec(query string, args ...driver.NamedValue) (sql.Result, error) {
	return tx.ExecContext(context.Background(), query, args...)
}

func (tx *HaSqliteTx) ExecContext(ctx context.Context, query string, args ...driver.NamedValue) (sql.Result, error) {
	if len(args) > MaxTupleParams {
		return nil, fmt.Errorf("too many parameters (%d) max = %d", len(args), MaxTupleParams)
	}
	parameters, err := proto.DriverNamedValueToParameters(args)
	if err != nil {
		return nil, fmt.Errorf("convert named value to parameters error %v", err)
	}
	statements := []*proto.Statement{{Sql: query, Parameters: parameters}}
	req := &proto.ExecRequest{Request: &proto.Request{
		DbId:       tx.dbId,
		Statements: statements,
	}}
	return proto.DBClientExecCheckResult(tx.client, ctx, req)
}

func (tx *HaSqliteTx) Commit() error {
	return fmt.Errorf("todo impl HaSqliteTx Commit")
}

func (tx *HaSqliteTx) Rollback() error {
	return fmt.Errorf("todo impl HaSqliteTx Rollback")
}
