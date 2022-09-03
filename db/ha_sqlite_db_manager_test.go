package db_test

import (
	"context"
	"database/sql/driver"
	"github.com/stretchr/testify/assert"
	"github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"sync"
	"testing"
)

type Store struct {
	db      *db.HaSqliteDBManager
	id      uint64
	t       *testing.T
	txToken string
}

func (store *Store) buildRequest(sql string, args ...driver.Value) *proto.Request {
	statements := make([]*proto.Statement, 1)
	parameters, err := proto.ValuesToParameters(args)
	assert.Nil(store.t, err)
	statements[0] = &proto.Statement{Sql: sql, Parameters: parameters}
	return &proto.Request{
		TxToken:    store.txToken,
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
		TxToken:    store.txToken,
		DbId:       store.id,
		Statements: statements,
	}
}

func (store *Store) cloneConn() *Store {
	return &Store{db: store.db, id: store.id, t: store.t}
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

func (store *Store) beginTx() {
	resp, err := store.db.BeginTx(context.Background(),
		&proto.BeginTxRequest{
			Type: proto.BeginTxRequest_TX_TYPE_BEGIN_LevelDefault,
			DbId: store.id,
		},
	)
	assert.Nil(store.t, err)
	assert.NotEmpty(store.t, resp.TxToken)
	store.txToken = resp.TxToken
}

func (store *Store) finishTx(txType proto.FinishTxRequest_Type) {
	_, err := store.db.FinishTx(context.Background(),
		&proto.FinishTxRequest{
			Type:    txType,
			DbId:    store.id,
			TxToken: store.txToken,
		},
	)
	assert.Nil(store.t, err)
	store.txToken = ""
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

func openDB(t *testing.T) *Store {
	store, err := db.NewHaSqliteDBManager()
	assert.Nilf(t, err, "NewHaSqliteDBManager")
	openResp, err := store.Open(context.Background(), &proto.OpenRequest{Dsn: ":memory:"})
	assert.Nilf(t, err, "store.Open")
	return &Store{db: store, id: openResp.DbId, t: t}
}

func Test_OpenDB(t *testing.T) {
	store := openDB(t)
	assert.NotEqual(t, 0, store.id)
	log.Printf("openResp.DbId:%d", store.id)
}

func Test_Exec(t *testing.T) {
	store := openDB(t)
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
	store := openDB(t)
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
	store := openDB(t)
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

func Test_Tx(t *testing.T) {
	store := openDB(t)
	store.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	store.beginTx()
	store.assertExecBatch(
		"INSERT INTO foo(name) VALUES(\"data 1\")",
		"INSERT INTO foo(name) VALUES(\"data 2\")",
		"INSERT INTO foo(name) VALUES(\"data 3\")",
		"INSERT INTO foo(name) VALUES(\"data 4\")",
		"INSERT INTO foo(name) VALUES(\"data 5\")",
	)
	resp := store.query("SELECT * FROM foo WHERE name = ?", "data 1")
	assert.Equal(t, 1, len(resp.Result[0].Values))
	store.finishTx(proto.FinishTxRequest_TX_TYPE_ROLLBACK)
	resp = store.query("SELECT * FROM foo WHERE name = ?", "data 1")
	assert.Equal(t, 0, len(resp.Result[0].Values))
	store.beginTx()
	store.assertExecBatch(
		"INSERT INTO foo(name) VALUES(\"data 1\")",
		"INSERT INTO foo(name) VALUES(\"data 2\")",
		"INSERT INTO foo(name) VALUES(\"data 3\")",
		"INSERT INTO foo(name) VALUES(\"data 4\")",
		"INSERT INTO foo(name) VALUES(\"data 5\")",
	)
	resp = store.query("SELECT * FROM foo WHERE name = ?", "data 1")
	assert.Equal(t, 1, len(resp.Result[0].Values))
	store.finishTx(proto.FinishTxRequest_TX_TYPE_COMMIT)
	resp = store.query("SELECT * FROM foo WHERE name = ?", "data 1")
	assert.Equal(t, 1, len(resp.Result[0].Values))
}

func Test_TxMixOtherQuery(t *testing.T) {
	store := openDB(t)
	store2 := store.cloneConn()
	store.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	store.beginTx()
	store.exec("INSERT INTO foo(name) VALUES(?)", "data 1")
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		resp := store2.query("SELECT * FROM foo WHERE name = ?", "data 1")
		assert.Equal(t, 0, len(resp.Result[0].Values))
		wg.Done()
	}()
	go func() {
		resp := store2.query("SELECT * FROM foo WHERE name = ?", "data 1")
		assert.Equal(t, 0, len(resp.Result[0].Values))
		wg.Done()
	}()
	resp := store.query("SELECT * FROM foo WHERE name = ?", "data 1")
	assert.Equal(t, 1, len(resp.Result[0].Values))
	store.finishTx(proto.FinishTxRequest_TX_TYPE_ROLLBACK)

	wg.Wait()
	store2.beginTx()
	wg.Add(1)
	go func() {
		store.exec("INSERT INTO foo(name) VALUES(?)", "data 1")
		resp := store.query("SELECT * FROM foo WHERE name = ?", "data 1")
		assert.Equal(t, 2, len(resp.Result[0].Values))
		wg.Done()
	}()
	store2.exec("INSERT INTO foo(name) VALUES(?)", "data 1")
	resp = store2.query("SELECT * FROM foo WHERE name = ?", "data 1")
	assert.Equal(t, 1, len(resp.Result[0].Values))
	store2.finishTx(proto.FinishTxRequest_TX_TYPE_COMMIT)
	wg.Wait()
}

func Test_TxBatch(t *testing.T) {
	store := openDB(t)
	var wg sync.WaitGroup
	count := 10
	wg.Add(count)
	store.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	for i := 0; i < count; i++ {
		go func() {
			next := store.cloneConn()
			next.beginTx()
			next.exec("INSERT INTO foo(name) VALUES(?)", "data 1")
			resp := next.query("SELECT * FROM foo WHERE name = ?", "data 1")
			assert.Equal(t, 1, len(resp.Result[0].Values))
			next.finishTx(proto.FinishTxRequest_TX_TYPE_ROLLBACK)
			wg.Done()
			resp = next.query("SELECT * FROM foo WHERE name = ?", "data 1")
			assert.Equal(t, 0, len(resp.Result[0].Values))
		}()
		wg.Add(1)
		go func() {
			resp := store.query("SELECT * FROM foo WHERE name = ?", "data 1")
			assert.Equal(t, 0, len(resp.Result[0].Values))
			wg.Done()
		}()
	}
	wg.Wait()
	store.exec("INSERT INTO foo(name) VALUES(?)", "data 1")
	resp := store.query("SELECT * FROM foo WHERE name = ?", "data 1")
	assert.Equal(t, 1, len(resp.Result[0].Values))
}
