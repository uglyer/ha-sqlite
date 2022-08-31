package db_test

import (
	"context"
	"database/sql/driver"
	"github.com/stretchr/testify/assert"
	"github.com/uglyer/ha-sqlite/db"
	ha_driver "github.com/uglyer/ha-sqlite/driver"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"testing"
)

type Store struct {
	db *db.HaSqliteDB
	id uint64
	t  *testing.T
}

func (store *Store) buildRequest(sql string, args ...driver.Value) *proto.Request {
	statements := make([]*proto.Statement, 1)
	parameters, err := ha_driver.ValuesToParameters(args)
	assert.Nil(store.t, err)
	statements[0] = &proto.Statement{Sql: sql, Parameters: parameters}
	return &proto.Request{
		DbId:       store.id,
		Statements: statements,
	}
}

func (store *Store) buildRequestBatch(sql ...string) *proto.Request {
	statements := make([]*proto.Statement, len(sql))
	for i, s := range sql {
		statements[i] = &proto.Statement{Sql: s}
	}
	return &proto.Request{
		DbId:       store.id,
		Statements: statements,
	}
}

func (store *Store) exec(sql string, args ...driver.Value) *proto.ExecResponse {
	req := store.buildRequest(sql, args...)
	resp, err := store.db.Exec(context.Background(), &proto.ExecRequest{Request: req})
	assert.Nil(store.t, err)
	return resp
}

func (store *Store) execBatch(sql ...string) *proto.ExecResponse {
	req := store.buildRequestBatch(sql...)
	resp, err := store.db.Exec(context.Background(), &proto.ExecRequest{Request: req})
	assert.Nil(store.t, err)
	return resp
}

func (store *Store) query(sql string, args ...driver.Value) *proto.QueryResponse {
	req := store.buildRequest(sql, args...)
	resp, err := store.db.Query(context.Background(), &proto.QueryRequest{Request: req})
	assert.Nil(store.t, err)
	assert.Equal(store.t, 1, len(resp.Result))
	assert.Empty(store.t, resp.Result[0].Error)
	return resp
}

func (store *Store) assertExec(sql string, args ...driver.Value) {
	resp := store.exec(sql, args...)

	for i, res := range resp.Result {
		assert.Emptyf(store.t, res.Error, "Error exec #(%d):%s,sql:%s", i, res.Error, sql)
	}
}

func (store *Store) assertExecBatch(sql ...string) {
	resp := store.execBatch(sql...)
	for i, res := range resp.Result {
		assert.Emptyf(store.t, res.Error, "Error exec #(%d):%s,sql:%s", i, res.Error, sql)
	}
}

func (store *Store) assertExecCheckEffect(target *proto.ExecResult, sql string, args ...driver.Value) {
	resp := store.exec(sql, args...)
	for i, res := range resp.Result {
		assert.Emptyf(store.t, res.Error, "Error exec #(%d):%s,sql:%s", i, res.Error, sql)
		assert.Equal(store.t, target.RowsAffected, res.RowsAffected, "预期的RowsAffected不一致，期望：%d,实际：%d，sql: %s", target.RowsAffected, res.RowsAffected, sql)
		assert.Equal(store.t, target.LastInsertId, res.LastInsertId, "预期的LastInsertId不一致，期望：%d,实际：%d，sql: %s", target.LastInsertId, res.LastInsertId, sql)
	}
}

func openDB(t *testing.T) (*Store, error) {
	store, err := db.NewHaSqliteDB()
	assert.Nilf(t, err, "NewHaSqliteDB")
	openResp, err := store.Open(context.Background(), &proto.OpenRequest{Dsn: ":memory:"})
	assert.Nilf(t, err, "store.Open")
	return &Store{db: store, id: openResp.DbId, t: t}, nil
}

func Test_OpenDB(t *testing.T) {
	store, err := openDB(t)
	assert.Nilf(t, err, "打开数据库")
	assert.NotEqual(t, 0, store.id)
	log.Printf("openResp.DbId:%d", store.id)
}

func Test_Exec(t *testing.T) {
	store, err := openDB(t)
	assert.Nilf(t, err, "打开数据库")
	store.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	store.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 1},
		"INSERT INTO foo(name) VALUES(?)", "test1")
	store.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 2},
		"INSERT INTO foo(name) VALUES(?)", "test2")
	store.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"INSERT INTO foo(name) VALUES(?)", "test3")
	store.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"UPDATE foo set name=? where id = ?", "update test1", 1)
	store.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"DELETE from foo where id = ?", 3)
}

func Test_ExecBatch(t *testing.T) {
	store, err := openDB(t)
	assert.Nilf(t, err, "打开数据库")
	store.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	store.assertExecBatch(
		"INSERT INTO foo(name) VALUES(\"data 1\")",
		"INSERT INTO foo(name) VALUES(\"data 2\")",
		"INSERT INTO foo(name) VALUES(\"data 3\")",
		"INSERT INTO foo(name) VALUES(\"data 4\")",
		"INSERT INTO foo(name) VALUES(\"data 5\")",
	)
}

func Test_Query(t *testing.T) {
	store, err := openDB(t)
	assert.Nilf(t, err, "打开数据库")
	store.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	store.assertExecBatch(
		"INSERT INTO foo(name) VALUES(\"data 1\")",
		"INSERT INTO foo(name) VALUES(\"data 2\")",
		"INSERT INTO foo(name) VALUES(\"data 3\")",
		"INSERT INTO foo(name) VALUES(\"data 4\")",
		"INSERT INTO foo(name) VALUES(\"data 5\")",
	)
	resp := store.query("SELECT * FROM foo WHERE name = ?", "data 1")
	assert.Equal(t, 1, len(resp.Result[0].Values))
}
