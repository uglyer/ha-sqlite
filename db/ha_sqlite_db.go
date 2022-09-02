package db

import (
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
	mtx                sync.Mutex
	txMtx              sync.RWMutex
	dbIndex            uint64
	dbFilenameTokenMap map[string]uint64
	dbMap              map[uint64]*sql.DB
	txMap              map[uint64]*txInfo
}

type txInfo struct {
	tx        *sql.Tx
	ch        chan struct{}
	token     string
	waitCount int32
	mtx       sync.Mutex
	wg        *sync.WaitGroup
}

func (tx *txInfo) callNext() {
	tx.mtx.Lock()
	defer tx.mtx.Unlock()
	tx.waitCount--
	log.Printf("callNext:%d", tx.waitCount)
	if tx.waitCount == 0 {
		close(tx.ch)
		return
	} else if tx.waitCount > 0 {
		// 大于0才通知下一个
		tx.ch <- struct{}{}
	}
}

func (tx *txInfo) wait() {
	tx.mtx.Lock()
	tx.waitCount++
	log.Printf("wait-callNext:%d", tx.waitCount)
	tx.mtx.Unlock()
	<-tx.ch
}

// TODO 使用系统信息管理 db(memory or disk) 用于存放dsn、dbId、本地文件路径、拉取状态(本地、S3远端)、版本号、最后一次更新时间、最后一次查询时间、快照版本 等信息

func NewHaSqliteDB() (*HaSqliteDB, error) {
	return &HaSqliteDB{
		dbIndex:            0,
		dbFilenameTokenMap: make(map[string]uint64),
		dbMap:              make(map[uint64]*sql.DB),
		txMap:              make(map[uint64]*txInfo),
	}, nil
}

// Open 打开数据库
func (d *HaSqliteDB) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	d.mtx.Lock()
	defer d.mtx.Unlock()
	if token, ok := d.dbFilenameTokenMap[req.Dsn]; ok {
		return &proto.OpenResponse{DbId: token}, nil
	}
	db, err := sql.Open("sqlite3", req.Dsn)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDB")
	}
	db.SetMaxOpenConns(1)
	d.dbIndex++
	token := d.dbIndex
	d.dbFilenameTokenMap[req.Dsn] = token
	d.dbMap[token] = db
	return &proto.OpenResponse{DbId: token}, nil
}

// Exec 执行数据库命令
func (d *HaSqliteDB) Exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	d.mtx.Lock()
	fmt.Println("Exec get tx")
	tx, txok := d.getTx(req.Request.DbId)
	fmt.Println("Exec get tx ok")
	log.Printf("id:%d,tx:%s,txok:%v", req.Request.DbId, req.Request.TxToken, txok)
	db, dbok := d.dbMap[req.Request.DbId]
	if !txok && !dbok {
		d.mtx.Unlock()
		return nil, fmt.Errorf("get db error : %d", req.Request.DbId)
	}
	if txok && req.Request.TxToken == "" {
		// 如果不含 tx token 的事件, 等待事务结束后执行
		// 先解锁, 避免全局死锁
		d.mtx.Unlock()
		tx.wg.Wait()
		d.mtx.Lock()
		// 置空, 此请求非事务操作
		tx = nil
		txok = false
	} else if txok && tx.token != req.Request.TxToken {
		d.mtx.Unlock()
		return nil, fmt.Errorf("tx token error")
	}
	fmt.Println("Exec get tx ok@2")
	defer d.mtx.Unlock()
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
		if txok {
			fmt.Println("Exec get tx ok@3")
			r, err = tx.tx.ExecContext(c, ss, parameters...)
			fmt.Println("Exec get tx ok@4")
			if err != nil {
				handleError(result, err)
				continue
			}
		} else {
			r, err = db.ExecContext(c, ss, parameters...)
			if err != nil {
				handleError(result, err)
				continue
			}
		}

		if r == nil {
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
func (d *HaSqliteDB) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	d.mtx.Lock()
	tx, txok := d.getTx(req.Request.DbId)
	db, dbok := d.dbMap[req.Request.DbId]
	if !txok && !dbok {
		d.mtx.Unlock()
		return nil, fmt.Errorf("get db error : %d", req.Request.DbId)
	}
	if txok && req.Request.TxToken == "" {
		// 如果不含 tx token 的事件, 等待事务结束后执行
		// 先解锁, 避免全局死锁
		d.mtx.Unlock()
		tx.wg.Wait()
		d.mtx.Lock()
		// 置空, 此请求非事务操作
		tx = nil
		txok = false
	} else if txok && tx.token != req.Request.TxToken {
		d.mtx.Unlock()
		return nil, fmt.Errorf("tx token error")
	}
	defer d.mtx.Unlock()
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
		if txok {
			rs, err = tx.tx.QueryContext(c, query, parameters...)
			if err != nil {
				rows.Error = err.Error()
				allRows = append(allRows, rows)
				continue
			}
		} else {
			rs, err = db.QueryContext(c, query, parameters...)
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

func (d *HaSqliteDB) getTx(dbId uint64) (*txInfo, bool) {
	d.txMtx.Lock()
	defer d.txMtx.Unlock()
	tx, ok := d.txMap[dbId]
	return tx, ok
}

func (d *HaSqliteDB) deleteTx(dbId uint64) {
	d.txMtx.Lock()
	defer d.txMtx.Unlock()
	delete(d.txMap, dbId)
}

// BeginTx 开始事务执行
func (d *HaSqliteDB) BeginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	d.mtx.Lock()
	db, ok := d.dbMap[req.DbId]
	d.mtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("get db error : %d", req.DbId)
	}
	d.txMtx.Lock()
	beforeTx, ok := d.txMap[req.DbId]
	token := uuid.New().String()
	if ok {
		// 等待上一个的结束事务事件
		fmt.Println("等待上一个的结束事务事件")
		d.txMtx.Unlock()
		beforeTx.wait()
		d.txMtx.Lock()
		fmt.Println("上一个的结束")
	}
	fmt.Println("创建事务")
	defer d.txMtx.Unlock()
	tx, err := db.BeginTx(c, req.TxOptions())
	if err != nil {
		return nil, err
	}
	if ok {
		// 复用同一个对象, 确保处理同时发起2个以上的事务执行能正确接收事件
		//nextTxInfo.ch = beforeTx.ch
		//nextTxInfo.wg = beforeTx.wg
		//nextTxInfo.waitCount = beforeTx.waitCount
		log.Printf("waitCount:%d", beforeTx.waitCount)
		beforeTx.tx = tx
		beforeTx.token = token
		d.txMap[req.DbId] = beforeTx
		beforeTx.wg.Add(1)
	} else {
		nextTxInfo := &txInfo{tx: tx, token: token}
		nextTxInfo.ch = make(chan struct{}, 1)
		nextTxInfo.wg = &sync.WaitGroup{}
		nextTxInfo.waitCount = 0
		d.txMap[req.DbId] = nextTxInfo
		nextTxInfo.wg.Add(1)
	}
	return &proto.BeginTxResponse{TxToken: token}, nil
}

// FinishTx 开始事务执行
func (d *HaSqliteDB) FinishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	d.mtx.Lock()
	_, ok := d.dbMap[req.DbId]
	d.mtx.Unlock()
	if !ok {
		return nil, fmt.Errorf("get db error : %d", req.DbId)
	}
	beforeTx, ok := d.getTx(req.DbId)
	if !ok {
		return nil, fmt.Errorf("get tx error : %d", req.DbId)
	}
	if beforeTx.token != req.TxToken {
		return nil, fmt.Errorf("tx token error")
	}
	defer func() {
		// 结束后删除tx,并且通知下一个
		d.deleteTx(req.DbId)
		beforeTx.callNext()
		beforeTx.wg.Done()
	}()
	if req.Type == proto.FinishTxRequest_TX_TYPE_COMMIT {
		err := beforeTx.tx.Commit()
		if err != nil {
			return nil, fmt.Errorf("tx commit error : %v", err)
		}
		return &proto.FinishTxResponse{}, nil
	} else if req.Type == proto.FinishTxRequest_TX_TYPE_ROLLBACK {
		err := beforeTx.tx.Rollback()
		if err != nil {
			return nil, fmt.Errorf("tx rollback error : %v", err)
		}
		return &proto.FinishTxResponse{}, nil
	}
	panic(fmt.Sprintf("unknow tx type :%s", req.Type.String()))
}
