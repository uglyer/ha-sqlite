package store

import "C"
import (
	"database/sql"
	"fmt"
	_ "github.com/uglyer/go-sqlite3" // Go SQLite bindings with wal hook
	"sync"
)

// HaSqliteDBStore 使用系统信息管理 db(memory or disk)
// 用于存放dsn、dbId、本地文件路径、拉取状态(本地、S3远端)、版本号、最后一次更新时间、最后一次查询时间、快照版本 等信息
type HaSqliteDBStore struct {
	mtx sync.Mutex
	db  *sql.DB
}

type HaSqliteDBStoreDBItem struct {
	Id              uint64 `sqlite:"id"`
	Path            string `sqlite:"path"`
	DBVersion       uint64 `sqlite:"db_version"`
	SnapshotVersion uint64 `sqlite:"snapshot_version"`
	CreateTime      int64  `sqlite:"create_time"`
	UpdateTime      int64  `sqlite:"update_time"`
}

const dbName = "ha_sqlite"

// NewHaSqliteDBStore 创建新的数仓，会自动创建数据库
func NewHaSqliteDBStore() (*HaSqliteDBStore, error) {
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("create table ha_sqlite(id integer not null primary key, path text, db_version text,snapshot_version text,create_time integer,update_time integer)")
	if err != nil {
		return nil, err
	}
	return &HaSqliteDBStore{db: db}, nil
}

// CreateHaSqliteDBStoreFromSnapshot 从快照中恢复数仓
func CreateHaSqliteDBStoreFromSnapshot(b []byte) (*HaSqliteDBStore, error) {
	if !validSQLiteFile(b) {
		return nil, fmt.Errorf("bytes is not a SQLite file")
	}
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	//conn, err := db.Conn(context.Background())
	//if err != nil {
	//	return nil, err
	//}
	//if err := conn.Raw(func(driverConn interface{}) error {
	//	srcConn := driverConn.(*sqlite3.SQLiteConn)
	//	//srcConn.Backup()
	//	//err2 := srcConn.Deserialize(b, "")
	//	//if err2 != nil {
	//	//	return fmt.Errorf("DeserializeIntoMemory: %s", err.Error())
	//	//}
	//	//defer srcConn.Close()
	//	//
	//	//// Now copy from tmp database to the database this function will return.
	//	//dbConn, err3 := retDB.rwDB.Conn(context.Background())
	//	//if err3 != nil {
	//	//	return fmt.Errorf("DeserializeIntoMemory: %s", err.Error())
	//	//}
	//	//defer dbConn.Close()
	//	//
	//	//return dbConn.Raw(func(driverConn interface{}) error {
	//	//	dstConn := driverConn.(*sqlite3.SQLiteConn)
	//	//	return copyDatabaseConnection(dstConn, srcConn)
	//	//})
	//
	//}); err != nil {
	//	return nil, err
	//}
	return &HaSqliteDBStore{db: db}, nil
}

// Deserialize TODO causes the connection to disconnect from the current database
// and then re-open as an in-memory database based on the contents of the
// byte slice. If deserelization fails, error will contain the return code
// of the underlying SQLite API call.
//
// When this function returns, the connection is referencing database
// data in Go space, so the connection and associated database must be copied
// immediately if it is to be used further.
//
// See https://www.sqlite.org/c3ref/deserialize.html
func Deserialize(b []byte, schema string) error {
	//if schema == "" {
	//	schema = "main"
	//}
	//var zSchema *C.char
	//zSchema = C.CString(schema)
	//defer C.free(unsafe.Pointer(zSchema))

	//rc := C.sqlite3_deserialize(c.db, zSchema,
	//	(*C.uint8_t)(unsafe.Pointer(&b[0])),
	//	C.sqlite3_int64(len(b)), C.sqlite3_int64(len(b)), 0)
	//if rc != 0 {
	//	return fmt.Errorf("deserialize failed with return %v", rc)
	//}
	return nil
}

// validateSQLiteFile checks that the supplied data looks like a SQLite database
// file. See https://www.sqlite.org/fileformat.html
func validSQLiteFile(b []byte) bool {
	return len(b) > 13 && string(b[0:13]) == "SQLite format"
}

// getDBIdByPath 通过路径获取数据库 id
func (s *HaSqliteDBStore) getDBIdByPath(path string) (uint64, bool, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	var id uint64
	if err := s.db.QueryRow("select id from ha_sqlite where path = ? order by id asc limit 1 ", path).Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return id, false, fmt.Errorf("getDBIdByPath error:%v", err)
	}
	return id, true, nil
}
