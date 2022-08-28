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

func (store *Store) exec(sql string, args ...driver.Value) (*proto.ExecResponse, error) {
	req, err := store.buildRequest(sql, args...)
	if err != nil {
		return nil, fmt.Errorf("error buildRequest when exec:%v", err)
	}
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

func Test_ExecCreateTable(t *testing.T) {
	store, err := openDB()
	if err != nil {
		t.Fatalf("Error NewHaSqliteDB:%v", err)
	}
	store.assertExec(t, "CREATE TABLE foo (id integer not null primary key, name text)")
}
