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
	"sync"
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
	mtx   sync.Mutex
	Store *Node
	db    *sql.DB
	t     *testing.T
	tx    *sql.Tx
	url   string
}

type ThreeNodeDB struct {
	mtx   sync.Mutex
	db1   *HaDB
	db2   *HaDB
	db3   *HaDB
	index uint
}

func openThreeNodeDB(t *testing.T, nodePrefix string, deleteLog bool) *ThreeNodeDB {
	db1 := openSingleNodeDB(t, 31300, fmt.Sprintf("%s_NodeA", nodePrefix), true, 0)
	db2 := openSingleNodeDB(t, 31301, fmt.Sprintf("%s_NodeB", nodePrefix), true, 31300)
	db3 := openSingleNodeDB(t, 31302, fmt.Sprintf("%s_NodeC", nodePrefix), true, 31302)
	return &ThreeNodeDB{db1: db1, db2: db2, db3: db3}
}

func (d *ThreeNodeDB) stop() {
	d.db3.Store.Stop()
	d.db2.Store.Stop()
	d.db1.Store.Stop()
}

func (d *ThreeNodeDB) getDB() *HaDB {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	d.index++
	if d.index == 3 {
		d.index = 0
	}
	if d.index == 0 {
		return d.db1
	} else if d.index == 1 {
		return d.db2
	} else if d.index == 2 {
		return d.db3
	} else {
		return d.db1
	}
}

func openSingleNodeDB(t *testing.T, port int, nodeId string, deleteLog bool, joinPort int) *HaDB {
	var joinAddress = ""
	var bootstrap = true
	if joinPort > 0 {
		joinAddress = fmt.Sprintf("localhost:%d", joinPort)
		bootstrap = false
	}
	store := NewNode(t, &node.HaSqliteConfig{
		Address:       fmt.Sprintf("localhost:%d", port),
		RaftBootstrap: bootstrap,
		RaftId:        nodeId,
		DataPath:      "data",
		RaftAdmin:     true,
		JoinAddress:   joinAddress,
	}, deleteLog)
	go func() {
		store.Serve()
	}()
	if bootstrap {
		store.ctx.WaitHasLeader()
	}
	url := fmt.Sprintf("multi:///localhost:%d/:memory:", port)
	db, err := sql.Open("ha-sqlite", url)

	assert.Nilf(t, err, "ha-sqlite open error:%v", err)
	db.SetMaxIdleConns(runtime.NumCPU() * 2)
	db.SetMaxOpenConns(runtime.NumCPU() * 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*800)
	defer cancel()
	if bootstrap {
		go func() {
			err = db.PingContext(ctx)
			assert.Nilf(t, err, "ha-sqlite ping error:%v", err)
		}()
	}
	select {
	case <-ctx.Done():
		return &HaDB{db: db, Store: store, t: t, url: url}
	case <-time.After(time.Millisecond * 900):
		t.Fatalf("connect %s timeout", url)
		return &HaDB{db: db, t: t}
	}
}

func (store *HaDB) cloneConn() *HaDB {
	db, err := sql.Open("ha-sqlite", store.url)
	assert.Nil(store.t, err)
	db.SetMaxIdleConns(runtime.NumCPU() * 2)
	db.SetMaxOpenConns(runtime.NumCPU() * 2)
	return &HaDB{db: db, t: store.t, Store: store.Store, url: store.url}
}

func (store *HaDB) assertExec(sql string, args ...interface{}) {
	store.mtx.Lock()
	tx := store.tx
	store.mtx.Unlock()
	if tx != nil {
		_, err := tx.Exec(sql, args...)
		assert.Nil(store.t, err)
	} else {
		_, err := store.db.Exec(sql, args...)
		assert.Nil(store.t, err)
	}
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

func (store *HaDB) assertQuery(query string, args ...interface{}) *sql.Rows {
	store.mtx.Lock()
	tx := store.tx
	store.mtx.Unlock()
	var result *sql.Rows
	var err error
	if tx != nil {
		result, err = tx.Query(query, args...)
	} else {
		result, err = store.db.Query(query, args...)
	}
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

func (store *HaDB) beginTx() {
	store.mtx.Lock()
	defer store.mtx.Unlock()
	assert.Nil(store.t, store.tx)
	tx, err := store.db.BeginTx(context.Background(), &sql.TxOptions{Isolation: sql.LevelLinearizable, ReadOnly: false})
	assert.Nil(store.t, err)
	store.tx = tx
}

func (store *HaDB) finishTx(t proto.FinishTxRequest_Type) {
	store.mtx.Lock()
	defer store.mtx.Unlock()
	assert.NotNil(store.t, store.tx)
	if t == proto.FinishTxRequest_TX_TYPE_COMMIT {
		err := store.tx.Commit()
		assert.Nil(store.t, err)
	} else if t == proto.FinishTxRequest_TX_TYPE_ROLLBACK {
		err := store.tx.Rollback()
		assert.Nil(store.t, err)
	}
	store.tx = nil
}

func Test_SingleNodOpenDB(t *testing.T) {
	db := openSingleNodeDB(t, 31300, "Test_OpenDB", true, 0)
	defer db.Store.Stop()
}

func Test_SingleNodeExec(t *testing.T) {
	db := openSingleNodeDB(t, 31300, "Test_Exec", true, 0)
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

func Test_SingleNodeQuery(t *testing.T) {
	db := openSingleNodeDB(t, 31300, "Test_Query", true, 0)
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

func Test_ThreeNodeQuery(t *testing.T) {
	n := openThreeNodeDB(t, "Test_ThreeNodeQuery", true)
	defer n.stop()
	n.getDB().assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	n.getDB().assertExec("INSERT INTO foo(name) VALUES(?)", "test1")
	n.getDB().assertExec("INSERT INTO foo(name) VALUES(?)", "test")
	n.getDB().assertExec("INSERT INTO foo(name) VALUES(?)", "test")
	n.getDB().assertExec("INSERT INTO foo(name) VALUES(?)", "test")
	n.getDB().assertQueryColumns([]string{"id", "name"}, "SELECT * FROM `foo` WHERE name = ?", "test")
	n.getDB().assertQueryColumns([]string{"id", "name"}, "SELECT id,name FROM `foo` WHERE name = ?", "test")
	n.getDB().assertQueryColumns([]string{"id"}, "SELECT id FROM `foo` WHERE name = ?", "test")
	n.getDB().assertQueryColumns([]string{"name"}, "SELECT name FROM `foo` WHERE name = ?", "test")
	n.getDB().assertQueryColumns([]string{"NNN"}, "SELECT name as NNN FROM `foo` WHERE name = ?", "test")
	var id int
	var name string
	n.getDB().assertQueryCount(3, n.getDB().assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "test"), &id, &name)
	n.getDB().assertQueryValues(func(i int) {
		assert.Equal(t, i+2, id)
		assert.Equal(t, "test", name)
	}, n.getDB().assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "test"), &id, &name)
	n.getDB().assertQueryCount(1, n.getDB().assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "test1"), &id, &name)
	n.getDB().assertQueryValues(func(i int) {
		assert.Equal(t, 1, id)
		assert.Equal(t, "test1", name)
	}, n.getDB().assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "test1"), &id, &name)
}

func Test_SingleNodeExecPerformanceAsync(t *testing.T) {
	db := openSingleNodeDB(t, 31300, "Test_ExecPerformanceAsync", true, 0)
	defer db.Store.Stop()
	db.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	count := 1000
	start := time.Now()
	ch := make(chan struct{}, runtime.NumCPU()*2)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		ch <- struct{}{}
		go func() {
			defer wg.Done()
			_, err := db.db.Exec("INSERT INTO foo(name) VALUES(?)", "test")
			assert.Nil(t, err)
			<-ch
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("异步插入%d条记录耗时:%v,qps:%d", count, elapsed, int64(float64(count)/elapsed.Seconds()))
}

func Test_SingleNodeTx(t *testing.T) {
	var id int
	var name string
	db := openSingleNodeDB(t, 31300, "Test_SingleNodeTx", true, 0)
	defer db.Store.Stop()
	db.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	db.beginTx()
	db.assertExec("INSERT INTO foo(name) VALUES(?)", "data 1")
	db.assertQueryCount(1, db.assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "data 1"), &id, &name)
	db.finishTx(proto.FinishTxRequest_TX_TYPE_ROLLBACK)
	db.assertQueryCount(0, db.assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "data 1"), &id, &name)
	db.beginTx()
	db.assertExec("INSERT INTO foo(name) VALUES(?)", "data 1")
	db.assertQueryCount(1, db.assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "data 1"), &id, &name)
	db.finishTx(proto.FinishTxRequest_TX_TYPE_COMMIT)
	db.assertQueryCount(1, db.assertQuery("SELECT id,name FROM `foo` WHERE name = ?", "data 1"), &id, &name)
}

func Test_SingleNodeTxBatch(t *testing.T) {
	var id int
	var name string
	store := openSingleNodeDB(t, 31300, "Test_SingleNodeTxBatch", true, 0)
	var wg sync.WaitGroup
	count := 100
	wg.Add(count)
	store.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	insertCount := 0
	var insertCheckLock sync.Mutex
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			insertCheckLock.Lock()
			defer insertCheckLock.Unlock()
			store.assertExec("INSERT INTO foo(name) VALUES(?)", "data not tx")
			insertCount++
			store.assertQueryCount(insertCount, store.assertQuery("SELECT * FROM foo WHERE name = ?", "data not tx"), &id, &name)
		}()
		go func() {
			defer wg.Done()
			next := store.cloneConn()
			next.beginTx()
			next.assertExec("INSERT INTO foo(name) VALUES(?)", "data 1")
			next.assertQueryCount(1, next.assertQuery("SELECT * FROM foo WHERE name = ?", "data 1"), &id, &name)
			next.finishTx(proto.FinishTxRequest_TX_TYPE_ROLLBACK)
			next.assertQueryCount(0, next.assertQuery("SELECT * FROM foo WHERE name = ?", "data 1"), &id, &name)
		}()
	}
	wg.Wait()
	store.assertExec("INSERT INTO foo(name) VALUES(?)", "data 1")
	store.assertQueryCount(1, store.assertQuery("SELECT * FROM foo WHERE name = ?", "data 1"), &id, &name)
}
