package proto

import (
	"context"
	"database/sql/driver"
	"fmt"
	"google.golang.org/grpc"
)

type DBExecResult struct {
	rowsAffected int64
	lastInsertId int64
}

func (result *DBExecResult) LastInsertId() (int64, error) {
	return result.lastInsertId, nil
}

func (result *DBExecResult) RowsAffected() (int64, error) {
	return result.rowsAffected, nil
}

func DBClientExecCheckResult(client DBClient, ctx context.Context, in *ExecRequest, opts ...grpc.CallOption) (driver.Result, error) {
	resp, err := client.Exec(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("exec error: %v", err)
	}
	if resp == nil || len(resp.Result) == 0 {
		return nil, fmt.Errorf("exec without resp")
	}
	if resp.Result[0].Error != "" {
		return nil, fmt.Errorf("exec error:%s", resp.Result[0].Error)
	}
	return &DBExecResult{
		rowsAffected: resp.Result[0].RowsAffected,
		lastInsertId: resp.Result[0].LastInsertId,
	}, nil
}
