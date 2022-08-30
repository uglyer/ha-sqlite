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
	"log"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"
)

type RPCStore struct {
	sock       net.Listener
	db         *hadb.HaSqliteDB
	grpcServer *grpc.Server
}

func NewRPCStore(t *testing.T, port string) *RPCStore {
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	assert.Nil(t, err)
	store, err := hadb.NewHaSqliteDB()
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
	defer store.sock.Close()
	if err := store.grpcServer.Serve(store.sock); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

type HaDB struct {
	db *sql.DB
	t  *testing.T
}

func openDB(t *testing.T, port string) *HaDB {
	store := NewRPCStore(t, port)
	go func() {
		store.Serve()
	}()
	url := fmt.Sprintf("multi:///localhost:%s/:memory:", port)
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
		return &HaDB{db: db, t: t}
	case <-time.After(time.Millisecond * 900):
		t.Fatalf("connect %s timeout", url)
		return &HaDB{db: db, t: t}
	}
}

func (store *HaDB) assertExec(sql string, args ...interface{}) {
	_, err := store.db.Exec(sql, args...)
	assert.Nil(store.t, err)
}

func (store *HaDB) assertExecCheckEffect(target *proto.ExecResult, sql string, args ...interface{}) {
	result, err := store.db.Exec(sql, args...)
	assert.Nil(store.t, err)
	rowsAffected, err := result.RowsAffected()
	assert.Nil(store.t, err)
	lastInsertId, err := result.LastInsertId()
	assert.Nil(store.t, err)
	assert.Equal(store.t, target.RowsAffected, rowsAffected)
	assert.Equal(store.t, target.LastInsertId, lastInsertId)
}

func Test_OpenDB(t *testing.T) {
	openDB(t, "30330")
}

func Test_Exec(t *testing.T) {
	db := openDB(t, "30331")
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

func Test_ExecPerformanceSync(t *testing.T) {
	db := openDB(t, "30332")
	db.assertExec("CREATE TABLE foo (id integer not null primary key, name text)")
	count := 1000
	start := time.Now()
	for i := 0; i < count; i++ {
		db.assertExec("INSERT INTO foo(name) VALUES(?)", "test")
	}
	elapsed := time.Since(start)
	log.Printf("插入%d条记录耗时:%v,qps:%d", count, elapsed, int64(float64(count)/elapsed.Seconds()))
}

func Test_ExecPerformanceAsync(t *testing.T) {
	db := openDB(t, "30333")
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
	log.Printf("插入%d条记录耗时:%v,qps:%d", count, elapsed, int64(float64(count)/elapsed.Seconds()))
}
