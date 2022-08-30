package node_test

import (
	"database/sql"
	"fmt"
	_ "github.com/uglyer/ha-sqlite/driver"
	"github.com/uglyer/ha-sqlite/node"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"net"
	"runtime"
	"testing"
	"time"
)

type Node struct {
	ctx *node.HaSqliteContext
}

func NewNode(address string) (*Node, error) {
	config := &node.HaSqliteConfig{
		Address:       address,
		RaftBootstrap: true,
		RaftId:        "nodeTestA",
		DataPath:      "data",
		RaftAdmin:     true,
	}
	_, port, err := net.SplitHostPort(config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse local address (%q): %v", config.Address, err)
	}
	config.LocalPort = port
	ctx, err := node.NewHaSqliteContext(config)

	if err != nil {
		log.Fatalf("failed to start HaSqliteContext: %v", err)
	}
	return &Node{
		ctx: ctx,
	}, nil
}

func (store *Node) Serve() {
	defer store.ctx.Sock.Close()
	if err := store.ctx.GrpcServer.Serve(store.ctx.Sock); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

type HaDB struct {
	db *sql.DB
	t  *testing.T
}

func openDB(t *testing.T) *HaDB {
	store, err := NewNode("localhost:30333")
	if err != nil {
		t.Fatalf("启动rpc服务失败:%v", err)
	}
	go func() {
		store.Serve()
	}()
	time.Sleep(time.Duration(2) * time.Second)
	db, err := sql.Open("ha-sqlite", "multi:///localhost:30333/:memory:")
	if err != nil {
		t.Fatalf("ha-sqlite open error:%v", err)
	}
	db.SetMaxIdleConns(runtime.NumCPU() * 2)
	db.SetMaxOpenConns(runtime.NumCPU() * 2)
	err = db.Ping()
	if err != nil {
		t.Fatalf("ha-sqlite ping error:%v", err)
	}
	return &HaDB{db: db, t: t}
}

func (store *HaDB) assertExec(sql string, args ...interface{}) {
	_, err := store.db.Exec(sql, args...)
	if err != nil {
		store.t.Fatalf("Error exec:%v", err)
	}
}

func (store *HaDB) assertExecCheckEffect(target *proto.ExecResult, sql string, args ...interface{}) {
	result, err := store.db.Exec(sql, args...)
	if err != nil {
		store.t.Fatalf("Error exec:%v", err)
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		store.t.Fatalf("fail get RowsAffected:%v", err)
	}
	lastInsertId, err := result.LastInsertId()
	if err != nil {
		store.t.Fatalf("fail get LastInsertId:%v", err)
	}
	if rowsAffected != target.RowsAffected {
		store.t.Fatalf("预期的RowsAffected不一致，期望：%d,实际：%d，sql: %s", target.RowsAffected, rowsAffected, sql)
	} else if lastInsertId != target.LastInsertId {
		store.t.Fatalf("预期的LastInsertId不一致，期望：%d,实际：%d，sql: %s", target.LastInsertId, lastInsertId, sql)
	}
}

func Test_OpenDB(t *testing.T) {
	openDB(t)
}

func Test_Exec(t *testing.T) {
	db := openDB(t)
	db.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 1},
		"INSERT INTO foo(name) VALUES(?)", "test1")
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 2},
		"INSERT INTO foo(name) VALUES(?)", "test2")
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"INSERT INTO foo(name) VALUES(?)", "test3")
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"UPDATE foo set name=? where id = ?", "update test1", 1)
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"DELETE from foo where id = ?", 3)
}
