package driver_test

import (
	"database/sql"
	"fmt"
	hadb "github.com/uglyer/ha-sqlite/db"
	_ "github.com/uglyer/ha-sqlite/driver"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"testing"
)

type RPCStore struct {
	sock       net.Listener
	db         *hadb.HaSqliteDB
	grpcServer *grpc.Server
}

func NewRPCStore(port string) (*RPCStore, error) {
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		return nil, err
	}
	store, err := hadb.NewHaSqliteDB()
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	proto.RegisterDBServer(s, store)
	return &RPCStore{
		sock:       sock,
		db:         store,
		grpcServer: s,
	}, nil
}

func (store *RPCStore) Serve() {
	defer store.sock.Close()
	if err := store.grpcServer.Serve(store.sock); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func Test_OpenDB(t *testing.T) {
	store, err := NewRPCStore("30333")
	if err != nil {
		t.Fatalf("启动rpc服务失败:%v", err)
	}
	go func() {
		store.Serve()
	}()
	db, err := sql.Open("ha-sqlite", "multi:///localhost:30333/:memory:")
	if err != nil {
		t.Fatalf("ha-sqlite open error:%v", err)
	}
	err = db.Ping()
	if err != nil {
		t.Fatalf("ha-sqlite ping error:%v", err)
	}
}
