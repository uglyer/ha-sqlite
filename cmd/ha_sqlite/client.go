package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/mkideal/cli"
	"runtime"
	"time"
)

type HaClient struct {
	db  *sql.DB
	url string
}

func newHaClient(url string) (*HaClient, error) {
	db, err := sql.Open("ha-sqlite", url)
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(runtime.NumCPU() * 2)
	db.SetMaxOpenConns(runtime.NumCPU() * 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*800)
	defer cancel()
	err = db.PingContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("ha-sqlite ping error:%v", err)
	}
	select {
	case <-ctx.Done():
		return &HaClient{db: db, url: url}, nil
	case <-time.After(time.Millisecond * 900):
		return nil, fmt.Errorf("connect timeout")
	}
}

func (c *HaClient) query(ctx *cli.Context, q string) {
	rows, err := c.db.Query(q)
	if err != nil {
		ctx.String("query error:%v", err)
		return
	}
	cols, err := rows.Columns()
	if err != nil {
		ctx.String("query get columns error:%v", err)
		return
	}
	for _, col := range cols {
		ctx.String("query get columns col:%s", col)
	}
}

func (c *HaClient) exec(ctx *cli.Context, q string) {
	result, err := c.db.Exec(q)
	if err != nil {
		ctx.String("exec error:%v", err)
		return
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		ctx.String("exec get rowsAffected error:%v", err)
		return
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		ctx.String("exec get lastInsertId error:%v", err)
		return
	}
	ctx.String("exec success! rowsAffected:%v,lastInsertId:%v", rowsAffected, lastInsertId)
}
