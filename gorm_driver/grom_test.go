package gorm_driver

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	hadb "github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"io/ioutil"
	"net"
	"os"
	"testing"

	_ "github.com/uglyer/ha-sqlite/driver"
	"gorm.io/gorm"
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

func TestDialector(t *testing.T) {
	port := uint16(30032)
	store := NewRPCStore(t, port)
	tempFile, err := ioutil.TempFile("", "ha-sqlite-gorm-driver-test")
	assert.Nil(t, err)
	go func() {
		store.Serve()
		os.Remove(tempFile.Name())
	}()
	DSN := fmt.Sprintf("multi:///localhost:%d/", port)
	// This is the custom SQLite driver name.
	const CustomDriverName = "ha-sqlite"
	db, err := gorm.Open(&Dialector{
		DriverName: CustomDriverName,
		DSN:        DSN,
	}, &gorm.Config{})
	assert.Nil(t, err)
	assert.Nil(t, db.Exec("SELECT 1").Error)
	assert.Nil(t, db.Exec("HA CREATE DB testdb1;").Error)
	assert.Nil(t, db.Exec("HA USE testdb1;SELECT 1").Error)
	//pool := &DBPool{}
	//pool.getDB("xxx")
}

//type DBPool struct {
//	mtx   sync.Mutex
//	dbMap map[string]*gorm.DB
//}
//
//func (pool *DBPool) getDB(name string) (*gorm.DB, error) {
//	pool.mtx.Lock()
//	defer pool.mtx.Unlock()
//	db, ok := pool.dbMap[name]
//	if !ok {
//		db, err := gorm.Open(&Dialector{
//			DriverName: CustomDriverName,
//			DSN:        DSN,
//		}, &gorm.Config{})
//		if err != nil {
//			return nil, err
//		}
//		pool.dbMap[name] = db
//		return db, nil
//	}
//	return db, nil
//}
