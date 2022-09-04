package db

import (
	"container/list"
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3" // Go SQLite bindings
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"strings"
	"sync"
	"time"
)

type HaSqliteDB struct {
	dbMtx      sync.Mutex
	cmdListMtx sync.Mutex
	db         *sql.DB
	tx         *sql.Tx
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

func newHaSqliteDB(dataSourceName string) (*HaSqliteDB, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDB")
	}
	db.SetMaxOpenConns(1)
	return &HaSqliteDB{
		db: db,
	}, nil
}

// ApplyCmd 结束事务执行
func (d *HaSqliteDB) ApplyCmd(c context.Context, t cmdType, req interface{}, txToken string) (interface{}, error) {
	ch := make(chan *cmdResp, 1)
	defer close(ch)
	d.cmdListMtx.Lock()
	cmd := &cmdReq{
		txToken: txToken,
		c:       c,
		t:       t,
		req:     req,
		respCh:  ch,
	}
	d.cmdList.PushBack(cmd)
	d.cmdListMtx.Unlock()
	go d.runCmd()
	result := <-ch
	if t == cmdTypeFinishTx {
		// 事务结束触发重新执行任务
		go d.runCmd()
	}
	return result.resp, result.err
}

func (d *HaSqliteDB) getNextCmd(it *list.Element) *cmdReq {
	if it == nil {
		return nil
	}
	cmd := it.Value.(*cmdReq)
	d.dbMtx.Lock()
	hasTx := d.tx != nil
	d.dbMtx.Unlock()
	if hasTx && cmd.txToken == "" {
		// 跳过当前任务执行
		return d.getNextCmd(it.Next())
	} else if hasTx && cmd.txToken != d.txToken {
		d.cmdList.Remove(it)
		cmd.respCh <- &cmdResp{err: errors.New("tx token error")}
		return d.getNextCmd(it.Next())
	}
	d.cmdList.Remove(it)
	return cmd
}

func (d *HaSqliteDB) runCmd() {
	d.cmdListMtx.Lock()
	defer d.cmdListMtx.Unlock()
	for {
		if d.cmdList.Len() == 0 {
			break
		}
		it := d.cmdList.Front()
		cmd := d.getNextCmd(it)
		if cmd == nil {
			return
		}
		result := &cmdResp{}
		switch cmd.t {
		case cmdTypeExec:
			resp, err := d.exec(cmd.c, cmd.req.(*proto.ExecRequest))
			result.resp = resp
			result.err = err
		case cmdTypeQuery:
			resp, err := d.query(cmd.c, cmd.req.(*proto.QueryRequest))
			result.resp = resp
			result.err = err
		case cmdTypeBeginTx:
			resp, err := d.beginTx(cmd.c, cmd.req.(*proto.BeginTxRequest))
			result.resp = resp
			result.err = err
		case cmdTypeFinishTx:
			resp, err := d.finishTx(cmd.c, cmd.req.(*proto.FinishTxRequest))
			result.resp = resp
			result.err = err
		default:
			result.err = fmt.Errorf("unknow cmd type:%v", cmd.t)
		}
		cmd.respCh <- result
	}
}

// Exec 执行数据库命令
func (d *HaSqliteDB) exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	d.dbMtx.Lock()
	defer d.dbMtx.Unlock()
	var allResults []*proto.ExecResult
	// handleError sets the error field on the given result. It returns
	// whether the caller should continue processing or break.
	handleError := func(result *proto.ExecResult, err error) bool {
		result.Error = err.Error()
		allResults = append(allResults, result)
		return true
	}

	// Execute each statement.
	for _, stmt := range req.Request.Statements {
		ss := stmt.Sql
		if ss == "" {
			continue
		}

		result := &proto.ExecResult{}
		start := time.Now()

		parameters, err := proto.ParametersToValues(stmt.Parameters)
		if err != nil {
			if handleError(result, err) {
				continue
			}
			break
		}
		var r sql.Result
		if d.tx != nil {
			r, err = d.tx.Exec(ss, parameters...)
			if err != nil {
				log.Printf("handleError:%v", err)
				handleError(result, err)
				continue
			}
		} else {
			r, err = d.db.ExecContext(c, ss, parameters...)
			if err != nil {
				handleError(result, err)
				continue
			}
		}

		if r == nil {
			handleError(result, fmt.Errorf("result is nil"))
			continue
		}

		lid, err := r.LastInsertId()
		if err != nil {
			if handleError(result, err) {
				continue
			}
			break
		}
		result.LastInsertId = lid

		ra, err := r.RowsAffected()
		if err != nil {
			if handleError(result, err) {
				continue
			}
			break
		}
		result.RowsAffected = ra
		if req.Timings {
			result.Time = time.Now().Sub(start).Seconds()
		}
		allResults = append(allResults, result)
	}

	return &proto.ExecResponse{
		Result: allResults,
	}, nil
}

// Query 查询记录
func (d *HaSqliteDB) query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	d.dbMtx.Lock()
	defer d.dbMtx.Unlock()
	var allRows []*proto.QueryResult
	for _, stmt := range req.Request.Statements {
		query := stmt.Sql
		if query == "" {
			continue
		}

		rows := &proto.QueryResult{}
		start := time.Now()

		parameters, err := proto.ParametersToValues(stmt.Parameters)
		if err != nil {
			rows.Error = err.Error()
			allRows = append(allRows, rows)
			continue
		}
		var rs *sql.Rows
		if d.tx != nil {
			rs, err = d.tx.QueryContext(c, query, parameters...)
			if err != nil {
				rows.Error = err.Error()
				allRows = append(allRows, rows)
				continue
			}
		} else {
			rs, err = d.db.QueryContext(c, query, parameters...)
			if err != nil {
				rows.Error = err.Error()
				allRows = append(allRows, rows)
				continue
			}
		}
		defer rs.Close()

		columns, err := rs.Columns()
		if err != nil {
			return nil, err
		}

		types, err := rs.ColumnTypes()
		if err != nil {
			return nil, err
		}
		xTypes := make([]string, len(types))
		for i := range types {
			xTypes[i] = strings.ToLower(types[i].DatabaseTypeName())
		}

		for rs.Next() {
			dest := make([]interface{}, len(columns))
			ptrs := make([]interface{}, len(dest))
			for i := range ptrs {
				ptrs[i] = &dest[i]
			}
			if err := rs.Scan(ptrs...); err != nil {
				return nil, err
			}
			params, err := proto.NormalizeRowValues(dest, xTypes)
			if err != nil {
				return nil, err
			}
			rows.Values = append(rows.Values, &proto.QueryResult_Values{
				Parameters: params,
			})
		}

		// Check for errors from iterating over rows.
		if err := rs.Err(); err != nil {
			rows.Error = err.Error()
			allRows = append(allRows, rows)
			continue
		}

		if req.Timings {
			rows.Time = time.Now().Sub(start).Seconds()
		}

		rows.Columns = columns
		rows.Types = xTypes
		allRows = append(allRows, rows)
	}

	return &proto.QueryResponse{Result: allRows}, nil
}

// BeginTx 开始事务执行
func (d *HaSqliteDB) beginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	d.dbMtx.Lock()
	defer d.dbMtx.Unlock()
	token := uuid.New().String()
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	d.tx = tx
	d.txToken = token
	return &proto.BeginTxResponse{TxToken: token}, nil
}

// FinishTx 结束事务执行
func (d *HaSqliteDB) finishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	d.dbMtx.Lock()
	defer d.dbMtx.Unlock()
	if d.tx == nil {
		return nil, fmt.Errorf("tx is null")
	}
	if d.txToken != req.TxToken {
		return nil, fmt.Errorf("tx token is error")
	}
	defer func() {
		d.tx = nil
		d.txToken = ""
	}()
	if req.Type == proto.FinishTxRequest_TX_TYPE_COMMIT {
		err := d.tx.Commit()
		if err != nil {
			return nil, fmt.Errorf("tx commit error : %v", err)
		}
		return &proto.FinishTxResponse{}, nil
	} else if req.Type == proto.FinishTxRequest_TX_TYPE_ROLLBACK {
		err := d.tx.Rollback()
		if err != nil {
			return nil, fmt.Errorf("tx rollback error : %v", err)
		}
		return &proto.FinishTxResponse{}, nil
	}
	panic(fmt.Sprintf("unknow tx type :%s", req.Type.String()))
}
