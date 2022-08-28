package db

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/proto"
	"sync"
)

type HaSqliteDB struct {
	mtx                sync.RWMutex
	dbIndex            uint64
	dbFilenameTokenMap map[string]uint64
	dbMap              map[uint64]*sql.DB
}

func NewHaSqliteDB() (*HaSqliteDB, error) {
	return &HaSqliteDB{
		dbIndex:            0,
		dbFilenameTokenMap: make(map[string]uint64),
		dbMap:              make(map[uint64]*sql.DB),
	}, nil
}

// Open 打开数据库
func (d *HaSqliteDB) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if token, ok := d.dbFilenameTokenMap[req.Dsn]; ok {
		return &proto.OpenResponse{DbId: token}, nil
	}
	db, err := sql.Open("sqlite3", req.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDB")
	}
	db.SetMaxOpenConns(1)
	d.dbIndex++
	token := d.dbIndex
	d.dbFilenameTokenMap[req.Dsn] = token
	d.dbMap[token] = db
	return &proto.OpenResponse{DbId: token}, nil
}
