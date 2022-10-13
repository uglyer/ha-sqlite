package db

import (
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/proto"
	"path"
)

type HaSqliteRaftDBManager struct {
	HaSqliteDBManager
	queueMap map[uint64]*HaSqliteCmdQueue
	raft     *raft.Raft
	dataPath string
}

func NewHaSqliteRaftDBManager(raft *raft.Raft, dataPath string) *HaSqliteRaftDBManager {
	manager := &HaSqliteRaftDBManager{
		raft:     raft,
		queueMap: make(map[uint64]*HaSqliteCmdQueue),
		dataPath: dataPath,
	}
	manager.dbIndex = 0
	manager.dbFilenameTokenMap = make(map[string]uint64)
	manager.dbMap = make(map[uint64]*HaSqliteDB)
	return manager
}

// Open 打开数据库
func (d *HaSqliteRaftDBManager) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if token, ok := d.dbFilenameTokenMap[req.Dsn]; ok {
		return &proto.OpenResponse{DbId: token}, nil
	}
	dataSourceName := path.Join(d.dataPath, req.Dsn)
	db, err := newHaSqliteDB(dataSourceName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDBManager")
	}
	d.dbIndex++
	token := d.dbIndex
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
	d.dbFilenameTokenMap[req.Dsn] = token
	d.dbMap[token] = db
	d.queueMap[token] = NewHaSqliteCmdQueue(d.raft)
	return &proto.OpenResponse{DbId: token}, nil
}

// queueApplyRaftLog 队列应用日志
func (d *HaSqliteRaftDBManager) queueApplyRaftLog(c context.Context, t cmdType, req *[]byte, dbId uint64, txToken string) (interface{}, error) {
	d.mtx.Lock()
	queue, ok := d.queueMap[dbId]
	d.mtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("get queue error:%d", dbId)
	}
	return queue.queueApplyRaftLog(c, t, req, txToken)
}
