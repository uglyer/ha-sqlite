package db

import (
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/db/store"
	"github.com/uglyer/ha-sqlite/proto"
	"path"
)

type HaSqliteRaftDBManager struct {
	HaSqliteDBManager
	raft     *raft.Raft
	dataPath string
}

func NewHaSqliteRaftDBManager(raft *raft.Raft, dataPath string) (*HaSqliteRaftDBManager, error) {
	store, err := store.NewHaSqliteDBStore()
	if err != nil {
		return nil, err
	}
	manager := &HaSqliteRaftDBManager{
		raft:     raft,
		dataPath: dataPath,
	}
	manager.store = store
	manager.dbMap = make(map[int64]*HaSqliteDB)
	manager.dbLockedMap = make(map[int64]int)
	return manager, nil
}

// Open 打开数据库(不存在则创建)
func (d *HaSqliteRaftDBManager) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	token, ok, err := d.store.GetDBIdByPath(req.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to GetDBIdByPath NewHaSqliteDBManager")
	}
	if ok {
		return &proto.OpenResponse{DbId: token}, nil
	}
	dataSourceName := path.Join(d.dataPath, req.Dsn)
	db, err := newHaSqliteDB(dataSourceName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	token, err = d.store.CreateDBByPath(req.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to CreateDBByPath NewHaSqliteDBManager")
	}
	d.initWalHook(db, token)
	d.dbMap[token] = db
	return &proto.OpenResponse{DbId: token}, nil
}

func (d *HaSqliteRaftDBManager) GetDB(dbId int64) (*HaSqliteDB, bool, error) {
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
	storePath, err := d.store.GetDBPathById(dbId)
	if err != nil {
		return nil, false, err
	}
	dataSourceName := path.Join(d.dataPath, storePath)
	db, err = newHaSqliteDB(dataSourceName)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	d.initWalHook(db, dbId)
	d.dbMap[dbId] = db
	if lockedCount, ok := d.dbLockedMap[dbId]; ok {
		d.dbLockedMap[dbId] = lockedCount + 1
	} else {
		d.dbLockedMap[dbId] = 1
	}
	return db, true, nil
}

func (d *HaSqliteRaftDBManager) initWalHook(db *HaSqliteDB, token int64) {
	db.InitWalHook(func(b []byte) error {
		cmdBytes, err := proto.BytesToCommandBytes(proto.Command_COMMAND_TYPE_APPLY_WAL, token, b)
		if err != nil {
			return errors.Wrap(err, "error encode wal file")
		}
		af := d.raft.Apply(cmdBytes, applyTimeout)
		err = af.Error()
		if err != nil {
			return err
		}
		r := af.Response().(*fsmGenericResponse)
		if r.error != nil {
			return r.error
		}
		return nil
	})
}

// Exec 执行数据库命令
func (d *HaSqliteRaftDBManager) Exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	db, ok, err := d.GetDB(req.Request.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	return db.exec(c, req)
}

// Query 查询记录
func (d *HaSqliteRaftDBManager) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	db, ok, err := d.GetDB(req.Request.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	return db.query(c, req)
}

// BeginTx 开始事务执行
func (d *HaSqliteRaftDBManager) BeginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	db, ok, err := d.GetDB(req.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.DbId, err)
	}
	return db.beginTx(c, req)
}

// FinishTx 开始事务执行
func (d *HaSqliteRaftDBManager) FinishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	db, ok, err := d.GetDB(req.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.DbId, err)
	}
	defer d.TryClose(req.DbId)
	return db.finishTx(c, req)
}

// ApplyWal 应用日志
func (d *HaSqliteRaftDBManager) ApplyWal(c context.Context, dbId int64, b []byte) error {
	db, ok, err := d.GetDB(dbId)
	if !ok || err != nil {
		return fmt.Errorf("get db error : %d,err:%v", dbId, err)
	}
	defer d.TryClose(dbId)
	return db.ApplyWal(c, b)
}
