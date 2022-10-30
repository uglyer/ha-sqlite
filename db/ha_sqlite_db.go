package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	sqlite "github.com/uglyer/go-sqlite3" // Go SQLite bindings with wal hook
	"github.com/uglyer/ha-sqlite/db/walfs"
	"github.com/uglyer/ha-sqlite/proto"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type HaSqliteDB struct {
	txMtx          sync.Mutex
	walMtx         sync.Mutex
	db             *sql.DB
	dataSourceName string
	sourceWalFile  string
	txMap          map[string]*sql.Tx
	onApplyWal     func(b []byte) error
	useCount       int32
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

func NewHaSqliteDB(dataSourceName string) (*HaSqliteDB, error) {
	dir := path.Dir(dataSourceName)
	if dir != "" {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return nil, errors.Wrap(err, "failed to create dir")
		}
	}
	url := fmt.Sprintf("file:%s?_txlock=exclusive&_busy_timeout=30000&_synchronous=OFF&vfs=ha_sqlite_vfs", dataSourceName)
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

// addUseCount 添加引用次数
func (d *HaSqliteDB) addUseCount(delta int32) {
	atomic.AddInt32(&d.useCount, delta)
}

// hasUsed 是否有正在被引用的请求
func (d *HaSqliteDB) hasUsed() bool {
	return atomic.LoadInt32(&d.useCount) > 0
}

// TryClose 尝试关闭
func (d *HaSqliteDB) TryClose() (bool, error) {
	if d.hasUsed() {
		return false, fmt.Errorf("db has used")
	}
	success := d.txMtx.TryLock()
	defer d.txMtx.Unlock()
	if !success {
		return false, fmt.Errorf("db tx is locked")
	}
	success = d.walMtx.TryLock()
	defer d.walMtx.Unlock()
	if !success {
		return false, fmt.Errorf("db wal is locked")
	}
	if len(d.txMap) > 0 {
		return false, fmt.Errorf("has tx")
	}
	hasWal := vfs.rootMemFS.VfsHasWal(d.sourceWalFile)
	if hasWal {
		return false, fmt.Errorf("has wal data")
	}
	d.onApplyWal = nil
	err := d.db.Close()
	d.db = nil
	return true, err
}

// InitWalHook 执行数据库命令
func (d *HaSqliteDB) InitWalHook(onApplyWal func(b []byte) error) {
	d.onApplyWal = onApplyWal
}

// checkWal 调用前需要确保 wal 持有锁
func (d *HaSqliteDB) checkWal() error {
	buffer, err, needApplyLog, unlockFunc := vfs.rootMemFS.VfsPoll(d.sourceWalFile)
	if !needApplyLog {
		unlockFunc(walfs.WAL_UNLOCK_EVENT_NONE)
		return nil
	}
	if err != nil {
		unlockFunc(walfs.WAL_UNLOCK_EVENT_ROLLBACK)
		return fmt.Errorf("vfs poll error:%v", err)
	}
	if d.onApplyWal == nil {
		unlockFunc(walfs.WAL_UNLOCK_EVENT_ROLLBACK)
		return fmt.Errorf("onApplyWal is null")
	}
	err = d.onApplyWal(buffer)
	if err != nil {
		// 提交日志失败 回滚
		unlockFunc(walfs.WAL_UNLOCK_EVENT_ROLLBACK)
		return fmt.Errorf("onApplyWal error:%v", err)
	}
	// 已经成功提交日志, 提交至 frame
	unlockFunc(walfs.WAL_UNLOCK_EVENT_COMMIT)
	var row [3]int
	// 应用 wal_checkpoint 失败不影响数据一致性, todo 提交至 frame 内容需要快照
	if err := d.db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE);`).Scan(&row[0], &row[1], &row[2]); err != nil {
		return errors.Wrap(err, "wal_checkpoint TRUNCATE error")
	} else if row[0] != 0 {
		return errors.Wrap(err, "wal_checkpoint TRUNCATE error#1")
	}
	return nil
}

// Exec 执行数据库命令
func (d *HaSqliteDB) Exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	d.addUseCount(1)
	defer d.addUseCount(-1)
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
			return nil, fmt.Errorf("exec get tx error:%s", req.Request.TxToken)
		}
		tx = dbTx
	} else {
		d.walMtx.Lock()
		defer d.walMtx.Unlock()
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
		err := d.checkWal()
		if err != nil {
			return nil, fmt.Errorf("exec apply wal error:%v", err)
		}
	}
	return &proto.ExecResponse{
		Result: allResults,
	}, nil
}

// Query 查询记录
func (d *HaSqliteDB) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	d.addUseCount(1)
	defer d.addUseCount(-1)
	var tx *sql.Tx
	if req.Request.TxToken != "" {
		d.txMtx.Lock()
		defer d.txMtx.Unlock()
		dbTx, ok := d.txMap[req.Request.TxToken]
		if !ok {
			return nil, fmt.Errorf("query get tx error:%s", req.Request.TxToken)
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
func (d *HaSqliteDB) BeginTx(c context.Context, req *proto.BeginTxRequest) (*proto.BeginTxResponse, error) {
	d.addUseCount(1)
	defer d.addUseCount(-1)
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
func (d *HaSqliteDB) FinishTx(c context.Context, req *proto.FinishTxRequest) (*proto.FinishTxResponse, error) {
	d.addUseCount(1)
	defer d.addUseCount(-1)
	d.txMtx.Lock()
	defer d.txMtx.Unlock()
	tx, ok := d.txMap[req.TxToken]
	if !ok {
		return nil, fmt.Errorf("finishTx get tx error:%s", req.TxToken)
	}
	defer func() {
		delete(d.txMap, req.TxToken)
	}()
	if req.Type == proto.FinishTxRequest_TX_TYPE_COMMIT {
		err := tx.Commit()
		if err != nil {
			return nil, fmt.Errorf("tx commit error : %v", err)
		}
		// TODO 仅在提交时持有锁可能在极端情况下会导致事务过程中提交数据被错误回滚
		d.walMtx.Lock()
		defer d.walMtx.Unlock()
		err = d.checkWal()
		if err != nil {
			return nil, fmt.Errorf("tx commit apply wal error : %v", err)
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

// ApplyWal 应用 wal 日志 由 raft 触发写日志时,与 checkWal 会有200ms的时间差
func (d *HaSqliteDB) ApplyWal(c context.Context, b []byte) error {
	d.addUseCount(1)
	defer d.addUseCount(-1)
	d.walMtx.Lock()
	defer d.walMtx.Unlock()
	//fmt.Printf("applyWal:时间戳（毫秒）：%v;%d\n", time.Now().UnixMilli(), len(b))
	err := vfs.rootMemFS.VfsApplyLog(d.sourceWalFile, b)
	if err != nil {
		return errors.Wrap(err, "Error VfsApplyLog")
	}
	//dbCopy, err := sql.Open("sqlite3-wal", d.dataSourceName)
	//if err != nil {
	//	return errors.Wrap(err, "Error open db file when apply wal")
	//}
	//defer dbCopy.Close()
	//if _, err := dbCopy.Exec(`PRAGMA journal_mode = wal`); err != nil {
	//	return errors.Wrap(err, "Failed to set db copy journal mode")
	//}
	// 直接执行 wal_checkpoint 会处于死锁状态, 但先执行查询预计并执行 Scan 功能正常, 原因未知
	var count int
	if err := d.db.QueryRow("select count(*) from sqlite_master").Scan(&count); err != nil {
		// 如果错误, wal_checkpoint 一定失败, TODO 回滚
		return errors.Wrap(err, "Check sqlite_master VfsApplyLog")
	}
	var row [3]int
	if err := d.db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE);`).Scan(&row[0], &row[1], &row[2]); err != nil {
		//log.Printf("ApplyWal wal_checkpoint TRUNCATE error#1:%v", err)
		return errors.Wrap(err, "ApplyWal Fwal_checkpoint TRUNCATE error#1")
	} else if row[0] != 0 {
		//log.Printf("ApplyWal wal_checkpoint TRUNCATE error#2:%v", row[0])
		return errors.Wrap(err, "ApplyWal wal_checkpoint TRUNCATE error#2")
	}
	return nil
}
