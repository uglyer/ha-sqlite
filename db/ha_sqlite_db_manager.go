package db

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/proto"
	"sync"
	"time"
)

type HaSqliteDBManager struct {
	mtx                sync.Mutex
	dbIndex            uint64
	dbFilenameTokenMap map[string]uint64
	dbMap              map[uint64]*HaSqliteDB
}

// TODO 使用系统信息管理 db(memory or disk) 用于存放dsn、dbId、本地文件路径、拉取状态(本地、S3远端)、版本号、最后一次更新时间、最后一次查询时间、快照版本 等信息

func NewHaSqliteDBManager() (*HaSqliteDBManager, error) {
	return &HaSqliteDBManager{
		dbIndex:            0,
		dbFilenameTokenMap: make(map[string]uint64),
		dbMap:              make(map[uint64]*HaSqliteDB),
	}, nil
}

// Open 打开数据库
func (d *HaSqliteDBManager) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if token, ok := d.dbFilenameTokenMap[req.Dsn]; ok {
		return &proto.OpenResponse{DbId: token}, nil
	}
	db, err := newHaSqliteDB(req.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	d.dbIndex++
	token := d.dbIndex
	d.dbFilenameTokenMap[req.Dsn] = token
	d.dbMap[token] = db
	return &proto.OpenResponse{DbId: token}, nil
}

func (d *HaSqliteDBManager) getDB(dbId uint64) (*HaSqliteDB, bool) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	db, ok := d.dbMap[dbId]
	return db, ok
}

// Ping 验证服务连通性
func (ctx *HaSqliteDBManager) Ping(c context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	return &proto.PingResponse{
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// Exec 执行数据库命令
func (d *HaSqliteDBManager) Exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	db, ok := d.getDB(req.Request.DbId)
	if !ok {
		return nil, fmt.Errorf("get db error : %d", req.Request.DbId)
	}
	return db.exec(c, req)
}

// Query 查询记录
func (d *HaSqliteDBManager) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	db, ok := d.getDB(req.Request.DbId)
	if !ok {
		return nil, fmt.Errorf("get db error : %d", req.Request.DbId)
	}
	return db.query(c, req)
}

// BeginTx 开始事务执行
func (d *HaSqliteDBManager) BeginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	db, ok := d.getDB(req.DbId)
	if !ok {
		return nil, fmt.Errorf("get db error : %d", req.DbId)
	}
	return db.beginTx(c, req)
}

// FinishTx 开始事务执行
func (d *HaSqliteDBManager) FinishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	db, ok := d.getDB(req.DbId)
	if !ok {
		return nil, fmt.Errorf("get db error : %d", req.DbId)
	}
	return db.finishTx(c, req)
}

// ApplyWal 开始事务执行
func (d *HaSqliteDBManager) ApplyWal(c context.Context, dbId uint64, b []byte) error {
	db, ok := d.getDB(dbId)
	if !ok {
		return fmt.Errorf("get db error : %d", dbId)
	}
	return db.applyWal(c, b)
}
