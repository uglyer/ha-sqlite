package db_test

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/uglyer/ha-sqlite/db"
	ha_driver "github.com/uglyer/ha-sqlite/driver"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"testing"
)

type Store struct {
	db *db.HaSqliteDB
	id uint64
}

func (store *Store) buildRequest(sql string, args ...driver.Value) (*proto.Request, error) {
	statements := make([]*proto.Statement, 1)
	parameters, err := ha_driver.ValuesToParameters(args)
	if err != nil {
		return nil, err
	}
	statements[0] = &proto.Statement{Sql: sql, Parameters: parameters}
	return &proto.Request{
		DbId:       store.id,
		Statements: statements,
	}, nil
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

func (store *Store) exec(sql string, args ...driver.Value) (*proto.ExecResponse, error) {
	req, err := store.buildRequest(sql, args...)
	if err != nil {
		return nil, fmt.Errorf("error buildRequest when exec:%v", err)
	}
	return store.db.Exec(context.Background(), &proto.ExecRequest{Request: req})
}

func (store *Store) execBatch(sql ...string) (*proto.ExecResponse, error) {
	req := store.buildRequestBatch(sql...)
	return store.db.Exec(context.Background(), &proto.ExecRequest{Request: req})
}

func (store *Store) assertExec(t *testing.T, sql string, args ...driver.Value) {
	resp, err := store.exec(sql, args...)
	if err != nil {
		t.Fatalf("Error exec:%v", err)
	}
	for i, res := range resp.Result {
		if res.Error != "" {
			t.Fatalf("Error exec #(%d):%s", i, res.Error)
		}
	}
}

func (store *Store) assertExecBatch(t *testing.T, sql ...string) {
	resp, err := store.execBatch(sql...)
	if err != nil {
		t.Fatalf("Error exec:%v", err)
	}
	for i, res := range resp.Result {
		if res.Error != "" {
			t.Fatalf("Error exec #(%d):%s", i, res.Error)
		}
	}
}

func (store *Store) assertExecCheckEffect(t *testing.T, target *proto.ExecResult, sql string, args ...driver.Value) {
	resp, err := store.exec(sql, args...)
	if err != nil {
		t.Fatalf("Error exec:%v", err)
	}
	for i, res := range resp.Result {
		if res.Error != "" {
			t.Fatalf("Error exec #(%d):%s,sql:%s", i, res.Error, sql)
		}
		if res.RowsAffected != target.RowsAffected {
			t.Fatalf("预期的RowsAffected不一致，期望：%d,实际：%d，sql: %s", target.RowsAffected, res.RowsAffected, sql)
		} else if res.LastInsertId != target.LastInsertId {
			t.Fatalf("预期的LastInsertId不一致，期望：%d,实际：%d，sql: %s", target.LastInsertId, res.LastInsertId, sql)
		} else {
			log.Printf("sql:%s,exec coast:%f", sql, res.Time)
		}
	}
}

func openDB() (*Store, error) {
	store, err := db.NewHaSqliteDB()
	if err != nil {
		return nil, err
	}
	openResp, err := store.Open(context.Background(), &proto.OpenRequest{Dsn: ":memory:"})
	if err != nil {
		return nil, err
	}
	return &Store{db: store, id: openResp.DbId}, nil
}

func Test_OpenDB(t *testing.T) {
	store, err := openDB()
	if err != nil {
		t.Fatalf("Error NewHaSqliteDB:%v", err)
	}
	if store.id == 0 {
		t.Fatalf("Error open db:%v", err)
	}
	log.Printf("openResp.DbId:%d", store.id)
}

func Test_Exec(t *testing.T) {
	store, err := openDB()
	if err != nil {
		t.Fatalf("Error NewHaSqliteDB:%v", err)
	}
	store.assertExec(t, "CREATE TABLE foo (id integer not null primary key, name text)")
	store.assertExecCheckEffect(t, &proto.ExecResult{RowsAffected: 1, LastInsertId: 1},
		"INSERT INTO foo(name) VALUES(?)", "test1")
	store.assertExecCheckEffect(t, &proto.ExecResult{RowsAffected: 1, LastInsertId: 2},
		"INSERT INTO foo(name) VALUES(?)", "test2")
	store.assertExecCheckEffect(t, &proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"INSERT INTO foo(name) VALUES(?)", "test3")
	store.assertExecCheckEffect(t, &proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"UPDATE foo set name=? where id = ?", "update test1", 1)
	store.assertExecCheckEffect(t, &proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"DELETE from foo where id = ?", 3)
}

func Test_ExecBatch(t *testing.T) {
	store, err := openDB()
	if err != nil {
		t.Fatalf("Error NewHaSqliteDB:%v", err)
	}
	store.assertExec(t, "CREATE TABLE foo (id integer not null primary key, name text)")
	store.assertExecBatch(
		t,
		"INSERT INTO foo(name) VALUES(\"data 1\")",
		"INSERT INTO foo(name) VALUES(\"data 2\")",
		"INSERT INTO foo(name) VALUES(\"data 3\")",
		"INSERT INTO foo(name) VALUES(\"data 4\")",
		"INSERT INTO foo(name) VALUES(\"data 5\")",
	)
}
