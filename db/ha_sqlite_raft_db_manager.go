package db

import (
	"context"
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
	d.dbMap[token] = db
	return &proto.OpenResponse{DbId: token}, nil
}
