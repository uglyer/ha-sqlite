package db

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"sync"
	"time"
)

type HaSqliteCmdQueue struct {
	mtx        sync.Mutex
	raft       *raft.Raft
	cmdListMtx sync.Mutex
	txToken    string
	cmdList    list.List
	runCh      chan *cmdReq
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

// TODO 目前不满足事务隔离执行
func NewHaSqliteCmdQueue(raft *raft.Raft) *HaSqliteCmdQueue {
	queue := &HaSqliteCmdQueue{
		raft:    raft,
		txToken: "",
		runCh:   make(chan *cmdReq, 1),
	}
	go queue.runQueue()
	return queue
}

// queueApplyRaftLog 队列应用日志
func (q *HaSqliteCmdQueue) queueApplyRaftLog(c context.Context, t cmdType, req interface{}, txToken string) (interface{}, error) {
	ch := make(chan *cmdResp, 1)
	defer close(ch)
	cmd := &cmdReq{
		txToken: txToken,
		c:       c,
		t:       t,
		req:     req,
		respCh:  ch,
	}
	q.runCh <- cmd
	result := <-ch
	if t == cmdTypeFinishTx {
		// 事务结束触发重新执行任务
		//go q.runCmd()
		q.runCh <- nil
	}
	return result.resp, result.err
}

func (q *HaSqliteCmdQueue) getNextCmd(it *list.Element) *cmdReq {
	if it == nil {
		return nil
	}
	cmd := it.Value.(*cmdReq)
	//q.mtx.Lock()
	hasTx := q.txToken != ""
	txToken := q.txToken
	//q.mtx.Unlock()
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

// timeCost 耗时统计
func timeCost() func() {
	start := time.Now()
	return func() {
		tc := time.Since(start)
		fmt.Printf("time cost = %v\n", tc)
	}
}
func (q *HaSqliteCmdQueue) runQueue() {
	//q.cmdListMtx.Lock()
	//defer q.cmdListMtx.Unlock()
	for cmd := range q.runCh {
		//if q.cmdList.Len() == 0 {
		//	q.cmdListMtx.Unlock()
		//	continue
		//}
		hasTx := q.txToken != ""
		if !hasTx && cmd == nil && q.cmdList.Len() > 0 {
			// 如果为空, 从队列中获取
			it := q.cmdList.Front()
			if it == nil {
				continue
			}
			cmd = it.Value.(*cmdReq)
			q.cmdList.Remove(it)
		} else if hasTx && cmd.txToken == "" {
			// 如果有事务, 追加至队列中
			q.cmdList.PushBack(cmd)
			continue
		} else if hasTx && cmd.txToken != q.txToken {
			// 如果有事务但是 token 不一致， 不合法
			cmd.respCh <- &cmdResp{err: errors.New("tx token error")}
			continue
		}
		if cmd == nil {
			continue
		}
		if cmd.t == cmdTypeBeginTx || cmd.txToken != "" {
			q.runCmd(cmd)
		} else {
			go q.runCmd(cmd)
		}
	}
}

func (q *HaSqliteCmdQueue) runCmd(cmd *cmdReq) {
	result := &cmdResp{}
	af := q.raft.Apply(*cmd.req.(*[]byte), applyTimeout).(raft.ApplyFuture)
	if af.Error() != nil {
		result.err = af.Error()
	} else {
		//defer timeCost()()
		result.resp = af.Response()
		if cmd.t == cmdTypeBeginTx {
			//q.mtx.Lock()
			if resp := result.resp.(*fsmBeginTxResponse); resp.err == nil {
				q.txToken = resp.resp.TxToken
			}
			//q.mtx.Unlock()
		} else if cmd.t == cmdTypeFinishTx {
			//q.mtx.Lock()
			if resp := result.resp.(*fsmFinishTxResponse); resp.err == nil {
				q.txToken = ""
			}
			//q.mtx.Unlock()
		}
	}
	cmd.respCh <- result
	//return result
}
