package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	_ "github.com/uglyer/go-sqlite3" // Go SQLite bindings with wal hook
	"github.com/uglyer/ha-sqlite/proto"
	"strings"
	"sync"
	"time"
)

type HaSqliteDB struct {
	txMtx sync.Mutex
	db    *sql.DB
	txMap map[string]*sql.Tx
}

func newHaSqliteDB(dataSourceName string) (*HaSqliteDB, error) {
	url := fmt.Sprintf("%s?_txlock=exclusive&_busy_timeout=30000", dataSourceName)
	db, err := sql.Open("sqlite3", url)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDB")
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		return nil, errors.Wrap(err, "failed to set NewHaSqliteDB synchronous off")
	}
	_, err = db.Exec("PRAGMA journal_mode = MEMORY")
	if err != nil {
		return nil, errors.Wrap(err, "failed to set NewHaSqliteDB journal_mode MEMORY")
	}
	return &HaSqliteDB{
		db:    db,
		txMap: make(map[string]*sql.Tx),
	}, nil
}

// Exec 执行数据库命令
func (d *HaSqliteDB) exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	var allResults []*proto.ExecResult
	// handleError sets the error field on the given result. It returns
	// whether the caller should continue processing or break.
	handleError := func(result *proto.ExecResult, err error) bool {
		result.Error = err.Error()
		allResults = append(allResults, result)
		return true
	}
	var tx *sql.Tx
	if req.Request.TxToken != "" {
		d.txMtx.Lock()
		defer d.txMtx.Unlock()
		dbTx, ok := d.txMap[req.Request.TxToken]
		if !ok {
			return nil, fmt.Errorf("get tx error:%s", req.Request.TxToken)
		}
		tx = dbTx
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
		if tx != nil {
			r, err = tx.Exec(ss, parameters...)
			if err != nil {
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
	var tx *sql.Tx
	if req.Request.TxToken != "" {
		d.txMtx.Lock()
		defer d.txMtx.Unlock()
		dbTx, ok := d.txMap[req.Request.TxToken]
		if !ok {
			return nil, fmt.Errorf("get tx error:%s", req.Request.TxToken)
		}
		tx = dbTx
	}
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
		if tx != nil {
			rs, err = tx.QueryContext(c, query, parameters...)
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
	token := uuid.New().String()
	// 必须调用 begin , 否则返回 tx 会自动回滚
	tx, err := d.db.Begin()
	if err != nil {
		return nil, err
	}
	d.txMtx.Lock()
	defer d.txMtx.Unlock()
	d.txMap[token] = tx
	return &proto.BeginTxResponse{TxToken: token}, nil
}

// FinishTx 结束事务执行
func (d *HaSqliteDB) finishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	d.txMtx.Lock()
	defer d.txMtx.Unlock()
	tx, ok := d.txMap[req.TxToken]
	if !ok {
		return nil, fmt.Errorf("get tx error:%s", req.TxToken)
	}
	defer func() {
		delete(d.txMap, req.TxToken)
	}()
	if req.Type == proto.FinishTxRequest_TX_TYPE_COMMIT {
		err := tx.Commit()
		if err != nil {
			return nil, fmt.Errorf("tx commit error : %v", err)
		}
		return &proto.FinishTxResponse{}, nil
	} else if req.Type == proto.FinishTxRequest_TX_TYPE_ROLLBACK {
		err := tx.Rollback()
		if err != nil {
			return nil, fmt.Errorf("tx rollback error : %v", err)
		}
		return &proto.FinishTxResponse{}, nil
	}
	panic(fmt.Sprintf("unknow tx type :%s", req.Type.String()))
}
