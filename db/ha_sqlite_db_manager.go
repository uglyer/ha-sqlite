package db

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/db/store"
	"github.com/uglyer/ha-sqlite/log"
	"github.com/uglyer/ha-sqlite/proto"
	"path"
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
	config            *HaSqliteConfig
}

var defaultHaSqliteConfig = &HaSqliteConfig{
	Address:       "",
	DataPath:      "",
	ManagerDBPath: ":memory:",
}

// TODO 使用系统信息管理 db(memory or disk) 用于存放dsn、dbId、本地文件路径、拉取状态(本地、S3远端)、版本号、最后一次更新时间、最后一次查询时间、快照版本 等信息

func NewHaSqliteDBManager() (*HaSqliteDBManager, error) {
	return NewHaSqliteDBManagerWithConfig(defaultHaSqliteConfig)
}

func NewHaSqliteDBManagerWithConfig(config *HaSqliteConfig) (*HaSqliteDBManager, error) {
	s, err := store.NewHaSqliteDBStoreWithDataSourceName(config.ManagerDBPath)
	if err != nil {
		return nil, err
	}
	return &HaSqliteDBManager{
		store:       s,
		dbMap:       make(map[int64]*HaSqliteDB),
		dbLockedMap: make(map[int64]int),
		defaultOnApplyWal: func(b []byte) error {
			return nil
		},
		config: config,
	}, nil
}

func NewHaSqliteDBManagerWithDefault(onApplyWal func(b []byte) error) (*HaSqliteDBManager, error) {
	s, err := store.NewHaSqliteDBStore()
	if err != nil {
		return nil, err
	}
	return &HaSqliteDBManager{
		store:             s,
		dbMap:             make(map[int64]*HaSqliteDB),
		dbLockedMap:       make(map[int64]int),
		defaultOnApplyWal: onApplyWal,
		config:            defaultHaSqliteConfig,
	}, nil
}

// Open 打开数据库(不存在则创建)
func (d *HaSqliteDBManager) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	dbPath := path.Join(d.config.DataPath, req.Dsn)
	log.Info(fmt.Sprintf("db open:%v,path:%v", req.Dsn, dbPath))
	token, ok, err := d.store.GetDBIdByPath(req.Dsn)
	if err != nil {
		log.Error(fmt.Sprintf("failed to GetDBIdByPath(%s):%v", req.Dsn, err))
		return nil, errors.Wrap(err, "failed to GetDBIdByPath NewHaSqliteDBManager")
	}
	if ok {
		return &proto.OpenResponse{DbId: token}, nil
	}
	db, err := NewHaSqliteDB(dbPath)
	if err != nil {
		log.Error(fmt.Sprintf("failed to Open NewHaSqliteDBManager(%s):%v", req.Dsn, err))
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	defer func() {
		closed, _ := db.TryClose()
		if !closed {
			log.Warn(fmt.Sprintf("failed to Open TryClose(%s):%v", req.Dsn, err))
			db.InitWalHook(d.defaultOnApplyWal)
			d.dbMap[token] = db
		}
	}()
	log.Info(fmt.Sprintf("db create:%v", req.Dsn))
	token, err = d.store.CreateDBByPath(req.Dsn)
	if err != nil {
		log.Error(fmt.Sprintf("failed to Open CreateDBByPath(%s):%v", req.Dsn, err))
		return nil, errors.Wrap(err, "failed to CreateDBByPath NewHaSqliteDBManager")
	}
	return &proto.OpenResponse{DbId: token}, nil
}

func (d *HaSqliteDBManager) GetDB(req *proto.Request) (*HaSqliteDB, bool, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	var dbId = req.DbId
	if req.Dsn != "" {
		id, ok, err := d.store.GetDBIdByPath(req.Dsn)
		if !ok {
			return nil, false, fmt.Errorf("get db id by path error:%v", err)
		}
		dbId = id
		req.DbId = id
	}
	db, ok := d.dbMap[dbId]
	if ok {
		err := d.store.RefDBUpdateTimeById(dbId)
		if err != nil {
			log.Error(fmt.Sprintf("failed to RefDBUpdateTimeById(%d):%v", dbId, err))
			return nil, false, err
		}
		if lockedCount, ok := d.dbLockedMap[dbId]; ok {
			d.dbLockedMap[dbId] = lockedCount + 1
		} else {
			d.dbLockedMap[dbId] = 1
		}
		return db, ok, nil
	}
	if req.Dsn == "" {
		dsn, err := d.store.GetDBPathById(dbId)
		if err != nil {
			log.Error(fmt.Sprintf("failed to GetDBPathById(%d):%v", dbId, err))
			return nil, false, err
		}
		req.Dsn = dsn
	}
	db, err := NewHaSqliteDB(path.Join(d.config.DataPath, req.Dsn))
	if err != nil {
		log.Error(fmt.Sprintf("failed to open database NewHaSqliteDBManager(%s):%v", req.Dsn, err))
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
			log.Warn(fmt.Sprintf("TryClose error(%d):%v", dbId, err))
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
	db, ok, err := d.GetDB(req.Request)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	log.Debug(fmt.Sprintf("Exec(%d):%v", req.Request.DbId, req.Request.Statements))
	resp, err := db.Exec(c, req)
	if err != nil {
		log.Error(fmt.Sprintf("Exec error(%d):%v", req.Request.DbId, req.Request.Statements))
	} else if len(resp.Result) > 0 && resp.Result[0].Error != "" {
		log.Warn(fmt.Sprintf("Exec failed(%d):%v resp:%s", req.Request.DbId, req.Request.Statements, resp.Result[0].Error))
	}
	return resp, err
}

// Query 查询记录
func (d *HaSqliteDBManager) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	db, ok, err := d.GetDB(req.Request)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	log.Debug(fmt.Sprintf("Query(%d):%v", req.Request.DbId, req.Request.Statements))
	resp, err := db.Query(c, req)
	if err != nil {
		log.Error(fmt.Sprintf("Query error(%d):%v", req.Request.DbId, req.Request.Statements))
	} else if len(resp.Result) > 0 && resp.Result[0].Error != "" {
		log.Warn(fmt.Sprintf("Query failed(%d):%v resp:%s", req.Request.DbId, req.Request.Statements, resp.Result[0].Error))
	}
	return resp, err
}

// BeginTx 开始事务执行
func (d *HaSqliteDBManager) BeginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	db, ok, err := d.GetDB(req.Request)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	log.Debug(fmt.Sprintf("BeginTx(%d):%v", req.Request.DbId, req.Type))
	resp, err := db.BeginTx(c, req)
	if err != nil {
		log.Error(fmt.Sprintf("Query error(%d):%v", req.Request.DbId, req.Type))
	}
	return resp, err
}

// FinishTx 开始事务执行
func (d *HaSqliteDBManager) FinishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	db, ok, err := d.GetDB(req.Request)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	log.Debug(fmt.Sprintf("FinishTx(%d):%v", req.Request.DbId, req.Type))
	resp, err := db.FinishTx(c, req)
	if err != nil {
		log.Error(fmt.Sprintf("Query error(%d):%v", req.Request.DbId, req.Type))
	}
	return resp, err
}

// Snapshot 快照
func (d *HaSqliteDBManager) Snapshot(c context.Context, req *proto.SnapshotRequest) (*proto.SnapshotResponse, error) {
	_, ok, err := d.GetDB(req.Request)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	return nil, fmt.Errorf("todo impl Snapshot")
}

// Restore 恢复
func (d *HaSqliteDBManager) Restore(c context.Context, req *proto.RestoreRequest) (*proto.RestoreResponse, error) {
	_, ok, err := d.GetDB(req.Request)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	return nil, fmt.Errorf("todo impl Restore")
}

// ApplyWal 应用日志
func (d *HaSqliteDBManager) ApplyWal(c context.Context, dbId int64, b []byte) error {
	return fmt.Errorf("ApplyWal is deprecated")
	//db, ok, err := d.GetDB(dbId)
	//if !ok || err != nil {
	//	return fmt.Errorf("get db error : %d,err:%v", dbId, err)
	//}
	//log.Debug(fmt.Sprintf("ApplyWal(%d):%v", dbId, len(b)))
	//defer d.TryClose(dbId)
	//return db.ApplyWal(c, b)
}

func (d *HaSqliteDBManager) DBInfo(ctx context.Context, request *proto.DBInfoRequest) (*proto.DBInfoResponse, error) {
	return d.store.GetDBInfo(request)
}
