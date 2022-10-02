package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	sqlite "github.com/uglyer/go-sqlite3" // Go SQLite bindings with wal hook
	"github.com/uglyer/ha-sqlite/proto"
	"log"
	"strings"
	"sync"
	"time"
)

type HaSqliteDB struct {
	txMtx          sync.Mutex
	db             *sql.DB
	dataSourceName string
	sourceWalFile  string
	txMap          map[string]*sql.Tx
	onApplyWal     func(b []byte) error
}

var vfs = NewHaSqliteVFS()

func init() {
	sql.Register("sqlite3-wal", &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {
			if err := conn.SetFileControlInt("", sqlite.SQLITE_FCNTL_PERSIST_WAL, 1); err != nil {
				return fmt.Errorf("Unexpected error from SetFileControlInt(): %w", err)
			}
			return nil
		},
	})
	err := sqlite.RegisterVFS("ha_sqlite_vfs", vfs)
	if err != nil {
		panic(fmt.Sprintf("RegisterVFS error:%v", err))
	}
}

func newHaSqliteDB(dataSourceName string) (*HaSqliteDB, error) {
	// TODO 实现 VFS https://github.com/psanford/sqlite3vfs github.com/blang/vfs/memfs
	// TODO 启用 vfs 后与 wal 冲突
	url := fmt.Sprintf("file:%s?_txlock=exclusive&_busy_timeout=30000&_synchronous=OFF&vfs=ha_sqlite_vfs", dataSourceName)
	//url := fmt.Sprintf("file:%s?_txlock=exclusive&_busy_timeout=30000&_synchronous=OFF", dataSourceName)
	db, err := sql.Open("sqlite3", url)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDB")
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("PRAGMA synchronous = OFF")
	if err != nil {
		return nil, fmt.Errorf("set synchronous = OFF error:%v", err)
	}
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("set journal_mode = WAL error:%v", err)
	}

	sourceWalFile := fmt.Sprintf("%s-wal", dataSourceName)
	return &HaSqliteDB{
		dataSourceName: dataSourceName,
		sourceWalFile:  sourceWalFile,
		db:             db,
		txMap:          make(map[string]*sql.Tx),
	}, nil
}

// InitWalHook 执行数据库命令
func (d *HaSqliteDB) InitWalHook(onApplyWal func(b []byte) error) {
	d.onApplyWal = onApplyWal
}

func (d *HaSqliteDB) checkWal() error {
	buffer, hasWal := vfs.rootMemFS.GetFileBuffer(d.sourceWalFile)
	if !hasWal {
		return nil
	}
	if d.onApplyWal == nil {
		return fmt.Errorf("onApplyWal is null")
	}
	// 无论如何都置空(对于成功的事件,置空操作无任何副作用,对于失败的操作 与回滚一致)
	defer buffer.Truncate(0)
	b := buffer.Copy()
	err := d.onApplyWal(b)
	if err != nil {
		log.Printf("apply error:%v", err)
		return fmt.Errorf("apply wal error:%v", err)
	}
	return nil
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
	if req.Request.TxToken == "" {
		d.checkWal()
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
		d.checkWal()
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

// applyWal 应用 wal 日志
func (d *HaSqliteDB) applyWal(c context.Context, b []byte) error {
	walBuffer, hasBuffer := vfs.rootMemFS.GetFileBuffer(d.sourceWalFile)
	if hasBuffer {
		_, err := walBuffer.WriteAt(b, 0)
		if err != nil {
			return errors.Wrap(err, "Error copy to wal buffer")
		}
	} else {
		walFile, _, err := vfs.Open(d.sourceWalFile, sqlite.OpenCreate)
		if err != nil {
			return errors.Wrap(err, "Error open wal file")
		}
		defer walFile.Close()
		_, err = walFile.WriteAt(b, 0)
		if err != nil {
			return errors.Wrap(err, "Error write wal file")
		}
	}
	//dbCopy, err := sql.Open("sqlite3-wal", d.dataSourceName)
	//if err != nil {
	//	return errors.Wrap(err, "Error open db file when apply wal")
	//}
	//defer dbCopy.Close()
	//if _, err := dbCopy.Exec(`PRAGMA journal_mode = wal`); err != nil {
	//	return errors.Wrap(err, "Failed to set db copy journal mode")
	//}
	var row [3]int
	if err := d.db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE);`).Scan(&row[0], &row[1], &row[2]); err != nil {
		return errors.Wrap(err, "Failed to set db copy journal mode")
	} else if row[0] != 0 {
		return errors.Wrap(err, "Failed to set db copy journal mode")
	}
	return nil
}
