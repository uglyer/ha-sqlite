package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	sqlite "github.com/uglyer/go-sqlite3" // Go SQLite bindings with wal hook
	"github.com/uglyer/ha-sqlite/proto"
	"io/ioutil"
	"os"
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
}

var vfs *HaSqliteVFS

func init() {
	sql.Register("sqlite3-wal", &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {
			if err := conn.SetFileControlInt("", sqlite.SQLITE_FCNTL_PERSIST_WAL, 1); err != nil {
				return fmt.Errorf("Unexpected error from SetFileControlInt(): %w", err)
			}
			//conn.RegisterWalHook(func(s string, i int) int {
			//	// 仅txnState == SQLITE_TXN_NONE 会触发调用
			//	conn.WalCheckpointV2("main", sqlite.SQLITE_CHECKPOINT_TRUNCATE, 0, i)
			//	return sqlite.SQLITE_OK
			//})
			return nil
		},
	})
	vfs = NewHaSqliteVFS()
	err := sqlite.VFSRegister("ha_sqlite", vfs)
	if err != nil {
		panic(fmt.Sprintf("VFSRegister error:%v", err))
	}
}

func newHaSqliteDB(dataSourceName string) (*HaSqliteDB, error) {
	// TODO 实现 VFS https://github.com/psanford/sqlite3vfs github.com/blang/vfs/memfs
	// TODO 启用 vfs 后与 wal 冲突
	url := fmt.Sprintf("%s?_txlock=exclusive&_busy_timeout=30000&_journal_mode=wal&vfs=ha_sqlite&_synchronous=OFF", dataSourceName)
	db, err := sql.Open("sqlite3-wal", url)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open database NewHaSqliteDB")
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)

	sourceWalFile := fmt.Sprintf("%s-wal", dataSourceName)
	return &HaSqliteDB{
		dataSourceName: dataSourceName,
		sourceWalFile:  sourceWalFile,
		db:             db,
		txMap:          make(map[string]*sql.Tx),
	}, nil
}

// InitWalHook 执行数据库命令
func (d *HaSqliteDB) InitWalHook(onApplyWal func(b []byte) error) error {
	conn, err := d.db.Conn(context.Background())
	if err != nil {
		return errors.Wrap(err, "failed to get NewHaSqliteDB conn")
	}
	defer conn.Close()
	if err := conn.Raw(func(driverConn interface{}) error {
		srcConn := driverConn.(*sqlite.SQLiteConn)
		srcConn.RegisterWalHook(func(s string, i int) int {
			if onApplyWal == nil {
				return sqlite.SQLITE_OK
			}
			// TODO 触发应用 raft 日志
			//_, err := ioutil.ReadFile(d.sourceWalFile)
			//if err != nil {
			//	log.Fatalf("Failed to get wal file:", err)
			//}
			//err = onApplyWal(input)
			//if err != nil {
			//	log.Fatalf("Failed to apply wal file:", err)
			//}
			// 仅txnState == SQLITE_TXN_NONE 会触发调用
			srcConn.WalCheckpointV2("main", sqlite.SQLITE_CHECKPOINT_TRUNCATE, 0, i)
			return sqlite.SQLITE_OK
		})
		return nil
	}); err != nil {
		return err
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
	// TODO 检查 wal 日志文件
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

// applyWal 应用 wal 日志
func (d *HaSqliteDB) applyWal(c context.Context, b []byte) error {
	defer os.Remove(d.sourceWalFile)
	err := ioutil.WriteFile(d.sourceWalFile, b, 0644)
	if err != nil {
		return errors.Wrap(err, "Error write wal file")
	}
	dbCopy, err := sql.Open("sqlite3-wal", d.dataSourceName)
	if err != nil {
		return errors.Wrap(err, "Error open db file when apply wal")
	}
	defer dbCopy.Close()
	if _, err := dbCopy.Exec(`PRAGMA journal_mode = wal`); err != nil {
		return errors.Wrap(err, "Failed to set db copy journal mode")
	}
	var row [3]int
	if err := dbCopy.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE);`).Scan(&row[0], &row[1], &row[2]); err != nil {
		return errors.Wrap(err, "Failed to set db copy journal mode")
	} else if row[0] != 0 {
		return errors.Wrap(err, "Failed to set db copy journal mode")
	}
	return nil
}
