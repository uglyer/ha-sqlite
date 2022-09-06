package db

import (
	"container/list"
	"context"
	"errors"
	"github.com/hashicorp/raft"
	"sync"
)

type HaSqliteCmdQueue struct {
	mtx        sync.Mutex
	raft       *raft.Raft
	cmdListMtx sync.Mutex
	txToken    string
	cmdList    list.List
}

type cmdType int8

const (
	cmdTypeExec     cmdType = 0
	cmdTypeQuery    cmdType = 1
	cmdTypeBeginTx  cmdType = 2
	cmdTypeFinishTx cmdType = 3
)

type cmdReq struct {
	c       context.Context
	t       cmdType
	txToken string
	req     interface{}
	respCh  chan *cmdResp
}

type cmdResp struct {
	resp interface{}
	err  error
}

func NewHaSqliteCmdQueue(raft *raft.Raft) *HaSqliteCmdQueue {
	return &HaSqliteCmdQueue{
		raft:    raft,
		txToken: "",
	}
}

// queueApplyRaftLog 队列应用日志
func (q *HaSqliteCmdQueue) queueApplyRaftLog(c context.Context, t cmdType, req interface{}, txToken string) (interface{}, error) {
	ch := make(chan *cmdResp, 1)
	defer close(ch)
	q.cmdListMtx.Lock()
	cmd := &cmdReq{
		txToken: txToken,
		c:       c,
		t:       t,
		req:     req,
		respCh:  ch,
	}
	q.cmdList.PushBack(cmd)
	q.cmdListMtx.Unlock()
	go q.runCmd()
	result := <-ch
	if t == cmdTypeFinishTx {
		// 事务结束触发重新执行任务
		go q.runCmd()
	}
	return result.resp, result.err
}

func (q *HaSqliteCmdQueue) getNextCmd(it *list.Element) *cmdReq {
	if it == nil {
		return nil
	}
	cmd := it.Value.(*cmdReq)
	q.mtx.Lock()
	hasTx := q.txToken != ""
	txToken := q.txToken
	q.mtx.Unlock()
	if hasTx && cmd.txToken == "" {
		// 跳过当前任务执行
		return q.getNextCmd(it.Next())
	} else if hasTx && cmd.txToken != txToken {
		q.cmdList.Remove(it)
		cmd.respCh <- &cmdResp{err: errors.New("tx token error")}
		return q.getNextCmd(it.Next())
	}
	q.cmdList.Remove(it)
	return cmd
}

func (q *HaSqliteCmdQueue) runCmd() {
	q.cmdListMtx.Lock()
	defer q.cmdListMtx.Unlock()
	for {
		if q.cmdList.Len() == 0 {
			break
		}
		it := q.cmdList.Front()
		cmd := q.getNextCmd(it)
		if cmd == nil {
			return
		}
		result := &cmdResp{}
		af := q.raft.Apply(cmd.req.([]byte), applyTimeout).(raft.ApplyFuture)
		if af.Error() != nil {
			result.err = af.Error()
		} else {
			result.resp = af.Response()
		}
		cmd.respCh <- result
	}
}
