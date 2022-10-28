package db

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/db/store"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"sync"
	"time"
)

type HaSqliteDBManager struct {
	mtx   sync.Mutex
	store *store.HaSqliteDBStore
	//dbIndex            int64
	//dbFilenameTokenMap map[string]int64
	dbMap             map[int64]*HaSqliteDB
	dbLockedMap       map[int64]int
	defaultOnApplyWal func(b []byte) error
}

// TODO 使用系统信息管理 db(memory or disk) 用于存放dsn、dbId、本地文件路径、拉取状态(本地、S3远端)、版本号、最后一次更新时间、最后一次查询时间、快照版本 等信息

func NewHaSqliteDBManager() (*HaSqliteDBManager, error) {
	store, err := store.NewHaSqliteDBStore()
	if err != nil {
		return nil, err
	}
	return &HaSqliteDBManager{
		store:       store,
		dbMap:       make(map[int64]*HaSqliteDB),
		dbLockedMap: make(map[int64]int),
		defaultOnApplyWal: func(b []byte) error {
			return nil
		},
	}, nil
}

func NewHaSqliteDBManagerWithDefault(onApplyWal func(b []byte) error) (*HaSqliteDBManager, error) {
	store, err := store.NewHaSqliteDBStore()
	if err != nil {
		return nil, err
	}
	return &HaSqliteDBManager{
		store:             store,
		dbMap:             make(map[int64]*HaSqliteDB),
		dbLockedMap:       make(map[int64]int),
		defaultOnApplyWal: onApplyWal,
	}, nil
}

// Open 打开数据库(不存在则创建)
func (d *HaSqliteDBManager) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	token, ok, err := d.store.GetDBIdByPath(req.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to GetDBIdByPath NewHaSqliteDBManager")
	}
	if ok {
		return &proto.OpenResponse{DbId: token}, nil
	}
	db, err := newHaSqliteDB(req.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	token, err = d.store.CreateDBByPath(req.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to CreateDBByPath NewHaSqliteDBManager")
	}
	db.InitWalHook(d.defaultOnApplyWal)
	d.dbMap[token] = db
	return &proto.OpenResponse{DbId: token}, nil
}

func (d *HaSqliteDBManager) GetDB(dbId int64) (*HaSqliteDB, bool, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	db, ok := d.dbMap[dbId]
	if ok {
		err := d.store.RefDBUpdateTimeById(dbId)
		if err != nil {
			return nil, false, err
		}
		if lockedCount, ok := d.dbLockedMap[dbId]; ok {
			d.dbLockedMap[dbId] = lockedCount + 1
		} else {
			d.dbLockedMap[dbId] = 1
		}
		return db, ok, nil
	}
	path, err := d.store.GetDBPathById(dbId)
	if err != nil {
		return nil, false, err
	}
	db, err = newHaSqliteDB(path)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	db.InitWalHook(d.defaultOnApplyWal)
	d.dbMap[dbId] = db
	if lockedCount, ok := d.dbLockedMap[dbId]; ok {
		d.dbLockedMap[dbId] = lockedCount + 1
	} else {
		d.dbLockedMap[dbId] = 1
	}
	return db, true, nil
}

// TryClose 尝试关闭库释放内存
func (d *HaSqliteDBManager) TryClose(dbId int64) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if lockedCount, ok := d.dbLockedMap[dbId]; ok {
		if lockedCount == 1 {
			delete(d.dbLockedMap, dbId)
		} else {
			d.dbLockedMap[dbId] = lockedCount - 1
			return
		}
	}
	db, ok := d.dbMap[dbId]
	if !ok {
		return
	}
	ok, err := db.TryClose()
	if ok {
		delete(d.dbMap, dbId)
		if err != nil {
			log.Printf("TryClose error(%d):%v", dbId, err)
		}
	}
}

// Ping 验证服务连通性
func (ctx *HaSqliteDBManager) Ping(c context.Context, req *proto.PingRequest) (*proto.PingResponse, error) {
	return &proto.PingResponse{
		Timestamp: time.Now().UnixMilli(),
	}, nil
}

// Exec 执行数据库命令
func (d *HaSqliteDBManager) Exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	db, ok, err := d.GetDB(req.Request.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	return db.exec(c, req)
}

// Query 查询记录
func (d *HaSqliteDBManager) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	db, ok, err := d.GetDB(req.Request.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	return db.query(c, req)
}

// BeginTx 开始事务执行
func (d *HaSqliteDBManager) BeginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	db, ok, err := d.GetDB(req.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.DbId, err)
	}
	return db.beginTx(c, req)
}

// FinishTx 开始事务执行
func (d *HaSqliteDBManager) FinishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	db, ok, err := d.GetDB(req.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.DbId, err)
	}
	defer d.TryClose(req.DbId)
	return db.finishTx(c, req)
}

// ApplyWal 应用日志
func (d *HaSqliteDBManager) ApplyWal(c context.Context, dbId int64, b []byte) error {
	db, ok, err := d.GetDB(dbId)
	if !ok || err != nil {
		return fmt.Errorf("get db error : %d,err:%v", dbId, err)
	}
	defer d.TryClose(dbId)
	return db.ApplyWal(c, b)
}
