package node_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	_ "github.com/uglyer/ha-sqlite/driver"
	"github.com/uglyer/ha-sqlite/node"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

type Node struct {
	ctx    *node.HaSqliteContext
	config *node.HaSqliteConfig
}

func NewNode(t *testing.T, config *node.HaSqliteConfig, deleteLog bool) *Node {
	if deleteLog {
		baseDir := filepath.Join(config.DataPath, config.RaftId)
		err := os.RemoveAll(baseDir)
		if err != nil {
			log.Printf("RemoveAll failed:%v", err)
		}
	}
	_, port, err := net.SplitHostPort(config.Address)
	assert.Nilf(t, err, "failed to parse local address (%q): %v", config.Address, err)
	config.LocalPort = port
	ctx, err := node.NewHaSqliteContext(config)
	assert.Nilf(t, err, "failed to start HaSqliteContext: %v", err)
	return &Node{
		ctx:    ctx,
		config: config,
	}
}

func (store *Node) Serve() {
	store.ctx.GrpcServer.Serve(store.ctx.Sock)
}

func (store *Node) Stop() {
	store.ctx.Sock.Close()
	store.ctx.Raft.Shutdown()
}

type HaDB struct {
	Store *Node
	db    *sql.DB
	t     *testing.T
}

func openSingleNodeDB(t *testing.T, port int, nodeId string, deleteLog bool) *HaDB {
	store := NewNode(t, &node.HaSqliteConfig{
		Address:       fmt.Sprintf("localhost:%d", port),
		RaftBootstrap: true,
		RaftId:        nodeId,
		DataPath:      "data",
		RaftAdmin:     true,
	}, deleteLog)
	go func() {
		store.Serve()
	}()
	store.ctx.WaitHasLeader()
	url := fmt.Sprintf("multi:///localhost:%d/:memory:", port)
	db, err := sql.Open("ha-sqlite", url)

	assert.Nilf(t, err, "ha-sqlite open error:%v", err)
	db.SetMaxIdleConns(runtime.NumCPU() * 2)
	db.SetMaxOpenConns(runtime.NumCPU() * 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*800)
	defer cancel()
	go func() {
		err = db.PingContext(ctx)
		assert.Nilf(t, err, "ha-sqlite ping error:%v", err)
	}()
	select {
	case <-ctx.Done():
		return &HaDB{db: db, Store: store, t: t}
	case <-time.After(time.Millisecond * 900):
		t.Fatalf("connect %s timeout", url)
		return &HaDB{db: db, t: t}
	}
}

func (store *HaDB) assertExec(sql string, args ...interface{}) {
	_, err := store.db.Exec(sql, args...)
	assert.Nilf(store.t, err, "Error exec:%v", err)
}

func (store *HaDB) assertExecCheckEffect(target *proto.ExecResult, sql string, args ...interface{}) {
	result, err := store.db.Exec(sql, args...)
	assert.Nilf(store.t, err, "Error exec:%v", err)
	rowsAffected, err := result.RowsAffected()
	assert.Nilf(store.t, err, "fail get RowsAffected:%v", err)
	lastInsertId, err := result.LastInsertId()
	assert.Nilf(store.t, err, "fail get LastInsertId:%v", err)
	assert.Equal(store.t, target.RowsAffected, rowsAffected)
	assert.Equal(store.t, target.LastInsertId, lastInsertId)
}

func (store *HaDB) assertQuery(sql string, args ...interface{}) *sql.Rows {
	result, err := store.db.Query(sql, args...)
	assert.Nil(store.t, err)
	return result
}

func (store *HaDB) assertQueryColumns(targetColumns []string, sql string, args ...interface{}) {
	rows := store.assertQuery(sql, args...)
	defer rows.Close()
	columns, err := rows.Columns()
	assert.Nil(store.t, err)
	assert.Equal(store.t, len(targetColumns), len(columns))
	for i, v := range columns {
		assert.Equal(store.t, targetColumns[i], v)
	}
}

func (store *HaDB) assertQueryCount(count int, rows *sql.Rows, args ...interface{}) {
	i := 0
	for rows.Next() {
		err := rows.Scan(args...)
		assert.Nil(store.t, err)
		i++
	}
	assert.Equal(store.t, count, i)
}

func (store *HaDB) assertQueryValues(handler func(int), rows *sql.Rows, args ...interface{}) {
	i := 0
	for rows.Next() {
		err := rows.Scan(args...)
		assert.Nil(store.t, err)
		handler(i)
		i++
	}
}

func Test_SingleNodOpenDB(t *testing.T) {
	db := openSingleNodeDB(t, 31300, "Test_OpenDB", true)
	defer db.Store.Stop()
}

func Test_SingleNodExec(t *testing.T) {
	db := openSingleNodeDB(t, 31300, "Test_Exec", true)
	defer db.Store.Stop()
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

func Test_SingleNodQuery(t *testing.T) {
	db := openSingleNodeDB(t, 31300, "Test_Query", true)
	defer db.Store.Stop()
	db.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	db.assertExec("INSERT INTO foo(name) VALUES(?)", "test1")
	db.assertExec("INSERT INTO foo(name) VALUES(?)", "test")
	db.assertExec("INSERT INTO foo(name) VALUES(?)", "test")
	db.assertExec("INSERT INTO foo(name) VALUES(?)", "test")
	db.assertQueryColumns([]string{"id", "name"}, "SELECT * FROM `foo` WHERE name = ?", "test")
	db.assertQueryColumns([]string{"id", "name"}, "SELECT id,name FROM `foo` WHERE name = ?", "test")
	db.assertQueryColumns([]string{"id"}, "SELECT id FROM `foo` WHERE name = ?", "test")
	db.assertQueryColumns([]string{"name"}, "SELECT name FROM `foo` WHERE name = ?", "test")
	db.assertQueryColumns([]string{"NNN"}, "SELECT name as NNN FROM `foo` WHERE name = ?", "test")
	var id int
	var name string
	db.assertQueryCount(3, db.assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "test"), &id, &name)
	db.assertQueryValues(func(i int) {
		assert.Equal(t, i+2, id)
		assert.Equal(t, "test", name)
	}, db.assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "test"), &id, &name)
	db.assertQueryCount(1, db.assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "test1"), &id, &name)
	db.assertQueryValues(func(i int) {
		assert.Equal(t, 1, id)
		assert.Equal(t, "test1", name)
	}, db.assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "test1"), &id, &name)
}
