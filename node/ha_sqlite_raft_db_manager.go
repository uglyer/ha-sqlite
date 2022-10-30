package node

import (
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/db/store"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"path"
	"sync"
)

type HaSqliteRaftDBManager struct {
	mtx               sync.Mutex
	store             *store.HaSqliteDBStore
	dbMap             map[int64]*db.HaSqliteDB
	dbLockedMap       map[int64]int
	defaultOnApplyWal func(b []byte) error
	raft              *raft.Raft
	dataPath          string
}

func NewHaSqliteRaftDBManager(raft *raft.Raft, dataPath string) (*HaSqliteRaftDBManager, error) {
	store, err := store.NewHaSqliteDBStore()
	if err != nil {
		return nil, err
	}
	manager := &HaSqliteRaftDBManager{
		raft:        raft,
		dataPath:    dataPath,
		store:       store,
		dbMap:       make(map[int64]*db.HaSqliteDB),
		dbLockedMap: make(map[int64]int),
	}
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
	sdb, err := db.NewHaSqliteDB(dataSourceName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	token, err = d.store.CreateDBByPath(req.Dsn)
	defer func() {
		closed, _ := sdb.TryClose()
		if !closed {
			d.initWalHook(sdb, token)
			d.dbMap[token] = sdb
		}
	}()
	if err != nil {
		return nil, errors.Wrap(err, "failed to CreateDBByPath NewHaSqliteDBManager")
	}
	return &proto.OpenResponse{DbId: token}, nil
}

func (d *HaSqliteRaftDBManager) GetDB(dbId int64) (*db.HaSqliteDB, bool, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	sdb, ok := d.dbMap[dbId]
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
		return sdb, ok, nil
	}
	storePath, err := d.store.GetDBPathById(dbId)
	if err != nil {
		return nil, false, err
	}
	dataSourceName := path.Join(d.dataPath, storePath)
	sdb, err = db.NewHaSqliteDB(dataSourceName)
	if err != nil {
		return nil, false, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	d.initWalHook(sdb, dbId)
	d.dbMap[dbId] = sdb
	if lockedCount, ok := d.dbLockedMap[dbId]; ok {
		d.dbLockedMap[dbId] = lockedCount + 1
	} else {
		d.dbLockedMap[dbId] = 1
	}
	return sdb, true, nil
}

func (d *HaSqliteRaftDBManager) initWalHook(db *db.HaSqliteDB, token int64) {
	db.InitWalHook(func(b []byte) error {
		// TODO 应用日志存读取异常信息耗时约 27ms, walhook 在 check wal 中触发阻塞执行, 写入存在性能瓶颈
		//startTime := time.Now()
		//defer func() {
		//	log.Printf("apply:%v", time.Since(startTime))
		//}()
		cmdBytes, err := proto.BytesToCommandBytes(proto.Command_COMMAND_TYPE_APPLY_WAL, token, b)
		if err != nil {
			return errors.Wrap(err, "error encode wal file")
		}
		af := d.raft.Apply(cmdBytes, applyTimeout)
		//log.Printf("apply:%v", time.Since(startTime))
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

// TryClose 尝试关闭库释放内存
func (d *HaSqliteRaftDBManager) TryClose(dbId int64) {
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
	sdb, ok := d.dbMap[dbId]
	if !ok {
		return
	}
	ok, err := sdb.TryClose()
	if ok {
		delete(d.dbMap, dbId)
		if err != nil {
			log.Printf("TryClose error(%d):%v", dbId, err)
		}
	}
}

// Exec 执行数据库命令
func (d *HaSqliteRaftDBManager) Exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	sdb, ok, err := d.GetDB(req.Request.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	return sdb.Exec(c, req)
}

// Query 查询记录
func (d *HaSqliteRaftDBManager) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	db, ok, err := d.GetDB(req.Request.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.Request.DbId, err)
	}
	defer d.TryClose(req.Request.DbId)
	return db.Query(c, req)
}

// BeginTx 开始事务执行
func (d *HaSqliteRaftDBManager) BeginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	db, ok, err := d.GetDB(req.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.DbId, err)
	}
	return db.BeginTx(c, req)
}

// FinishTx 开始事务执行
func (d *HaSqliteRaftDBManager) FinishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	db, ok, err := d.GetDB(req.DbId)
	if !ok || err != nil {
		return nil, fmt.Errorf("get db error : %d,err:%v", req.DbId, err)
	}
	defer d.TryClose(req.DbId)
	return db.FinishTx(c, req)
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
