package db

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"github.com/uglyer/ha-sqlite/proto"
	"strings"
	"sync"
	"time"
)

type HaSqliteDB struct {
	mtx                sync.RWMutex
	dbIndex            uint64
	dbFilenameTokenMap map[string]uint64
	dbMap              map[uint64]*sql.DB
}

func NewHaSqliteDB() (*HaSqliteDB, error) {
	return &HaSqliteDB{
		dbIndex:            0,
		dbFilenameTokenMap: make(map[string]uint64),
		dbMap:              make(map[uint64]*sql.DB),
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
	defer d.mtx.Unlock()
	db, ok := d.dbMap[req.Request.DbId]
	if !ok {
		return nil, fmt.Errorf("get db error : %d", req.Request.DbId)
	}
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

		parameters, err := parametersToValues(stmt.Parameters)
		if err != nil {
			if handleError(result, err) {
				continue
			}
			break
		}

		r, err := db.ExecContext(c, ss, parameters...)
		if err != nil {
			if handleError(result, err) {
				continue
			}
			break
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
	defer d.mtx.Unlock()
	db, ok := d.dbMap[req.Request.DbId]
	if !ok {
		return nil, fmt.Errorf("get db error : %d", req.Request.DbId)
	}
	var allRows []*proto.QueryResult
	for _, stmt := range req.Request.Statements {
		sql := stmt.Sql
		if sql == "" {
			continue
		}

		rows := &proto.QueryResult{}
		start := time.Now()

		parameters, err := parametersToValues(stmt.Parameters)
		if err != nil {
			rows.Error = err.Error()
			allRows = append(allRows, rows)
			continue
		}

		rs, err := db.QueryContext(context.Background(), sql, parameters...)
		if err != nil {
			rows.Error = err.Error()
			allRows = append(allRows, rows)
			continue
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
			params, err := normalizeRowValues(dest, xTypes)
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
