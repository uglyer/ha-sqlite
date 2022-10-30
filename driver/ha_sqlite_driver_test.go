package driver_test

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/stretchr/testify/assert"
	hadb "github.com/uglyer/ha-sqlite/db"
	_ "github.com/uglyer/ha-sqlite/driver"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"io/ioutil"
	"log"
	"net"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"
)

type RPCStore struct {
	sock       net.Listener
	db         *hadb.HaSqliteDBManager
	grpcServer *grpc.Server
}

func NewRPCStore(t *testing.T, port uint16) *RPCStore {
	sock, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	assert.Nil(t, err)
	store, err := hadb.NewHaSqliteDBManager()
	assert.Nil(t, err)
	s := grpc.NewServer()
	proto.RegisterDBServer(s, store)
	return &RPCStore{
		sock:       sock,
		db:         store,
		grpcServer: s,
	}
}

func (store *RPCStore) Serve() {
	store.grpcServer.Serve(store.sock)
}

func (store *RPCStore) Close() {
	store.sock.Close()
}

type HaDB struct {
	mtx   sync.Mutex
	Store *RPCStore
	db    *sql.DB
	tx    *sql.Tx
	t     *testing.T
	url   string
}

func openDB(t *testing.T, port uint16) *HaDB {
	store := NewRPCStore(t, port)
	tempFile, err := ioutil.TempFile("", "ha-sqlite-driver-test")
	go func() {
		store.Serve()
		os.Remove(tempFile.Name())
	}()
	url := fmt.Sprintf("multi:///localhost:%d/%s", port, tempFile.Name())
	db, err := sql.Open("ha-sqlite", url)
	assert.Nil(t, err)
	db.SetMaxIdleConns(runtime.NumCPU() * 2)
	db.SetMaxOpenConns(runtime.NumCPU() * 2)
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*800)
	defer cancel()
	go func() {
		err = db.PingContext(ctx)
		assert.Nil(t, err)
	}()
	select {
	case <-ctx.Done():
		return &HaDB{db: db, t: t, Store: store, url: url}
	case <-time.After(time.Millisecond * 900):
		t.Fatalf("connect %s timeout", url)
		return &HaDB{db: db, t: t, url: url}
	}
}

func (store *HaDB) cloneConn() *HaDB {
	db, err := sql.Open("ha-sqlite", store.url)
	assert.Nil(store.t, err)
	db.SetMaxIdleConns(runtime.NumCPU() * 2)
	db.SetMaxOpenConns(runtime.NumCPU() * 2)
	return &HaDB{db: db, t: store.t, Store: store.Store}
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
	assert.Nil(store.t, err)
	rowsAffected, err := result.RowsAffected()
	assert.Nil(store.t, err)
	lastInsertId, err := result.LastInsertId()
	assert.Nil(store.t, err)
	assert.Equal(store.t, target.RowsAffected, rowsAffected)
	if target.LastInsertId > 0 {
		assert.Equal(store.t, target.LastInsertId, lastInsertId)
	}
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

func Test_OpenDB(t *testing.T) {
	db := openDB(t, 30330)
	defer db.Store.Close()
}

func Test_Exec(t *testing.T) {
	db := openDB(t, 30330)
	defer db.Store.Close()
	db.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 1},
		"INSERT INTO foo(name) VALUES(?)", "test1")
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 2},
		"INSERT INTO foo(name) VALUES(?)", "test2")
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: 3},
		"INSERT INTO foo(name) VALUES(?)", "test3")
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: -1},
		"UPDATE foo set name=? where id = ?", "update test1", 1)
	db.assertExecCheckEffect(&proto.ExecResult{RowsAffected: 1, LastInsertId: -1},
		"DELETE from foo where id = ?", 3)
}

func Test_ExecPerformanceSync(t *testing.T) {
	db := openDB(t, 30330)
	defer db.Store.Close()
	db.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	count := 1000
	start := time.Now()
	for i := 0; i < count; i++ {
		db.assertExec("INSERT INTO foo(name) VALUES(?)", "test")
	}
	elapsed := time.Since(start)
	log.Printf("顺序插入%d条记录耗时:%v,qps:%d", count, elapsed, int64(float64(count)/elapsed.Seconds()))
}

func Test_ExecPerformanceAsync(t *testing.T) {
	db := openDB(t, 30330)
	defer db.Store.Close()
	db.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	count := 10000
	start := time.Now()
	ch := make(chan struct{}, runtime.NumCPU()*2)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		ch <- struct{}{}
		go func() {
			defer wg.Done()
			db.assertExec("INSERT INTO foo(name) VALUES(?)", "test")
			<-ch
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("异步插入%d条记录耗时:%v,qps:%d", count, elapsed, int64(float64(count)/elapsed.Seconds()))
}

func Test_Query(t *testing.T) {
	db := openDB(t, 30330)
	defer db.Store.Close()
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

func Test_Tx(t *testing.T) {
	var id int
	var name string
	db := openDB(t, 30330)
	defer db.Store.Close()
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

func Test_TxBatch(t *testing.T) {
	var id int
	var name string
	store := openDB(t, 30330)
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
