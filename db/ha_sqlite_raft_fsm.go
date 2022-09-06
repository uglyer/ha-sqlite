package db

import (
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/uglyer/ha-sqlite/proto"
	gProto "google.golang.org/protobuf/proto"
	"io"
	"log"
	"sync"
	"time"
)

const (
	applyTimeout = 10 * time.Second
)

// HaSqliteRaftFSM Raft 生命周期相关接口实现
type HaSqliteRaftFSM struct {
	mtx   sync.RWMutex
	store *HaSqliteDBManager
	raft  *raft.Raft
}

//var _ raft.FSM = &HaSqliteRaftFSM{}

func NewHaSqliteRaftFSM() (*HaSqliteRaftFSM, error) {
	store, err := NewHaSqliteDBManager()
	if err != nil {
		return nil, err
	}
	return &HaSqliteRaftFSM{
		store: store,
	}, nil
}

func (fsm *HaSqliteRaftFSM) InitRaft(r *raft.Raft) {
	fsm.raft = r
}

func (fsm *HaSqliteRaftFSM) Apply(l *raft.Log) interface{} {
	fsm.mtx.Lock()
	defer fsm.mtx.Unlock()
	return fsm.applyCommand(l.Data)
}

func (fsm *HaSqliteRaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	log.Println("HaSqliteRaftFSM.Snapshot")
	// Make sure that any future calls to f.Apply() don't change the snapshot.
	return &HaSqliteSnapshot{}, nil
}

func (fsm *HaSqliteRaftFSM) Restore(r io.ReadCloser) error {
	log.Println("HaSqliteRaftFSM.Restore")

	return nil
}

// Open 打开数据库
func (fsm *HaSqliteRaftFSM) applyCommand(data []byte) interface{} {
	var c proto.Command

	if err := gProto.Unmarshal(data, &c); err != nil {
		panic(fmt.Sprintf("failed to unmarshal cluster command: %s", err.Error()))
	}
	switch c.Type {
	case proto.Command_COMMAND_TYPE_OPEN:
		var req proto.OpenRequest
		if err := gProto.Unmarshal(c.SubCommand, &req); err != nil {
			panic(fmt.Sprintf("failed to unmarshal query subcommand: %s", err.Error()))
		}
		resp, err := fsm.store.Open(context.Background(), &req)
		return &fsmOpenResponse{resp: resp, err: err}
	case proto.Command_COMMAND_TYPE_EXEC:
		var req proto.ExecRequest
		if err := gProto.Unmarshal(c.SubCommand, &req); err != nil {
			panic(fmt.Sprintf("failed to unmarshal query subcommand: %s", err.Error()))
		}
		resp, err := fsm.store.Exec(context.Background(), &req)
		return &fsmExecResponse{resp: resp, err: err}
	case proto.Command_COMMAND_TYPE_BEGIN_TX:
		var req proto.BeginTxRequest
		if err := gProto.Unmarshal(c.SubCommand, &req); err != nil {
			panic(fmt.Sprintf("failed to unmarshal query subcommand: %s", err.Error()))
		}
		resp, err := fsm.store.BeginTx(context.Background(), &req)
		return &fsmBeginTxResponse{resp: resp, err: err}
	case proto.Command_COMMAND_TYPE_FINISH_TX:
		var req proto.FinishTxRequest
		if err := gProto.Unmarshal(c.SubCommand, &req); err != nil {
			panic(fmt.Sprintf("failed to unmarshal query subcommand: %s", err.Error()))
		}
		resp, err := fsm.store.FinishTx(context.Background(), &req)
		return &fsmFinishTxResponse{resp: resp, err: err}
	default:
		return &fsmGenericResponse{error: fmt.Errorf("unknow cmd type:%s", c.Type.String())}
	}
}

type fsmGenericResponse struct {
	error error
}

type fsmOpenResponse struct {
	resp *proto.OpenResponse
	err  error
}

type fsmExecResponse struct {
	resp *proto.ExecResponse
	err  error
}

type fsmBeginTxResponse struct {
	resp *proto.BeginTxResponse
	err  error
}

type fsmFinishTxResponse struct {
	resp *proto.FinishTxResponse
	err  error
}

// Open 打开数据库
func (fsm *HaSqliteRaftFSM) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	b, err := req.ToCommandBytes()
	if err != nil {
		return nil, err
	}
	af := fsm.raft.Apply(b, applyTimeout).(raft.ApplyFuture)
	if af.Error() != nil {
		return nil, af.Error()
	}
	r := af.Response().(*fsmOpenResponse)
	return r.resp, r.err
}

// Exec 执行数据库命令
func (fsm *HaSqliteRaftFSM) Exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	b, err := req.ToCommandBytes()
	if err != nil {
		return nil, err
	}
	af := fsm.raft.Apply(b, applyTimeout).(raft.ApplyFuture)
	if af.Error() != nil {
		return nil, af.Error()
	}
	r := af.Response().(*fsmExecResponse)
	return r.resp, r.err
}

// Query 查询记录
func (fsm *HaSqliteRaftFSM) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	return fsm.store.Query(c, req)
}

// BeginTx 开始事务执行
func (fsm *HaSqliteRaftFSM) BeginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	b, err := req.ToCommandBytes()
	if err != nil {
		return nil, err
	}
	af := fsm.raft.Apply(b, applyTimeout).(raft.ApplyFuture)
	if af.Error() != nil {
		return nil, af.Error()
	}
	r := af.Response().(*fsmBeginTxResponse)
	return r.resp, r.err
}

// FinishTx 开始事务执行
func (fsm *HaSqliteRaftFSM) FinishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	b, err := req.ToCommandBytes()
	if err != nil {
		return nil, err
	}
	af := fsm.raft.Apply(b, applyTimeout).(raft.ApplyFuture)
	if af.Error() != nil {
		return nil, af.Error()
	}
	r := af.Response().(*fsmFinishTxResponse)
	return r.resp, r.err
}
