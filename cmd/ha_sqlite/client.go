package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/mkideal/cli"
	"github.com/mkideal/pkg/textutil"
	"runtime"
	"time"
)

type HaClient struct {
	db     *sql.DB
	url    string
	dbName string
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

func (c *HaClient) prefix() string {
	return fmt.Sprintf("ha_sqlite(%s)>", c.dbName)
}

func (c *HaClient) setDbName(ctx *cli.Context, dbName string) {
	c.dbName = dbName
	c.query(ctx, "HA CREATE DB ?", dbName)
}

func (c *HaClient) queryWithDBName(ctx *cli.Context, q string) {
	c.query(ctx, fmt.Sprintf("HA USE ?;%s", q), c.dbName)
}

func (c *HaClient) query(ctx *cli.Context, q string, v ...interface{}) {
	rows, err := c.db.Query(q, v...)
	if err != nil {
		ctx.String("query error:%v\n", err)
		return
	}
	result, err := parseSqlRows(rows)
	if err != nil {
		ctx.String("query error:%v\n", err)
		return
	}
	textutil.WriteTable(ctx, result, &textutil.DefaultStyle{})
}

func (c *HaClient) execWithDBName(ctx *cli.Context, q string) {
	c.query(ctx, fmt.Sprintf("HA USE ?;%s", q), c.dbName)
}

func (c *HaClient) exec(ctx *cli.Context, q string, v ...interface{}) {
	result, err := c.db.Exec(q, v)
	if err != nil {
		ctx.String("exec error:%v\n", err)
		return
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		ctx.String("exec get rowsAffected error:%v\n", err)
		return
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		ctx.String("exec get lastInsertId error:%v\n", err)
		return
	}
	ctx.String("exec success! rowsAffected:%v,lastInsertId:%v\n", rowsAffected, lastInsertId)
}
