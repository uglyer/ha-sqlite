package proto

import (
	"context"
	"database/sql/driver"
	"fmt"
	"google.golang.org/grpc"
)

var printCoastTime = false

func SetIsPrintCoastTime(flag bool) {
	printCoastTime = flag
}

type DBExecResult struct {
	rowsAffected int64
	lastInsertId int64
	Time         float64
}

func (result *DBExecResult) LastInsertId() (int64, error) {
	return result.lastInsertId, nil
}

func (result *DBExecResult) RowsAffected() (int64, error) {
	return result.rowsAffected, nil
}

func DBClientExecCheckResult(client DBClient, ctx context.Context, in *ExecRequest, opts ...grpc.CallOption) (driver.Result, error) {
	resp, err := client.Exec(ctx, in, opts...)
	if err != nil {
		return nil, fmt.Errorf("exec error: %v", err)
	}
	if resp == nil || len(resp.Result) == 0 {
		return nil, fmt.Errorf("exec without resp")
	}
	if resp.Result[0].Error != "" {
		return nil, fmt.Errorf("exec error:%s", resp.Result[0].Error)
	}
	if printCoastTime {
		fmt.Printf("%v ms", resp.Result[0].Time)
	}
	return &DBExecResult{
		rowsAffected: resp.Result[0].RowsAffected,
		lastInsertId: resp.Result[0].LastInsertId,
	}, nil
}

func DBClientQueryCheckResult(client DBClient, ctx context.Context, in *QueryRequest, opts ...grpc.CallOption) (driver.Rows, error) {
	resp, err := client.Query(ctx, in, opts...)
	if err != nil {
		return nil, fmt.Errorf("exec error: %v", err)
	}
	if resp == nil || len(resp.Result) == 0 {
		return nil, fmt.Errorf("exec without resp")
	}
	if resp.Result[0].Error != "" {
		return nil, fmt.Errorf("exec error:%s", resp.Result[0].Error)
	}
	if printCoastTime {
		fmt.Printf("%v ms\n", resp.Result[0].Time)
	}
	return NewHaSqliteRowsFromSingleQueryResult(resp.Result[0]), nil
}
