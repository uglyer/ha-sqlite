package store

import "C"
import (
	"database/sql"
	"fmt"
	_ "github.com/uglyer/go-sqlite3" // Go SQLite bindings with wal hook
	"github.com/uglyer/ha-sqlite/proto"
	"reflect"
	"strings"
	"sync"
	"time"
)

// HaSqliteDBStore 使用系统信息管理 db(memory or disk)
// 用于存放dsn、dbId、本地文件路径、拉取状态(本地、S3远端)、版本号、最后一次更新时间、最后一次查询时间、快照版本 等信息
type HaSqliteDBStore struct {
	mtx sync.Mutex
	db  *sql.DB
}

type HaSqliteDBStoreDBItem struct {
	Id              uint64 `sqlite3_field:"id" sqlite3_default:"NOT NULL PRIMARY KEY AUTOINCREMENT" sqlite3_index:"UNIQUE INDEX"`
	Path            string `sqlite3_field:"path" sqlite3_default:"not null" sqlite3_index:"UNIQUE INDEX"`
	DBVersion       uint64 `sqlite3_field:"db_version" sqlite3_default:"not null default 0" sqlite3_index:"INDEX"`
	SnapshotVersion uint64 `sqlite3_field:"snapshot_version" sqlite3_default:"not null default 0" sqlite3_index:"INDEX"`
	CreateTime      int64  `sqlite3_field:"create_time" sqlite3_default:"not null" sqlite3_index:"INDEX"`
	UpdateTime      int64  `sqlite3_field:"update_time" sqlite3_default:"not null" sqlite3_index:"INDEX"`
}

const tableName = "ha_sqlite"

// NewHaSqliteDBStore 创建新的数仓，会自动创建数据库
func NewHaSqliteDBStore() (*HaSqliteDBStore, error) {
	return NewHaSqliteDBStoreWithDataSourceName(":memory:")
}

// NewHaSqliteDBStoreWithDataSourceName 创建新的数仓，会自动创建数据库
func NewHaSqliteDBStoreWithDataSourceName(dataSourceName string) (*HaSqliteDBStore, error) {
	db, err := sql.Open("sqlite3", dataSourceName)
	if err != nil {
		return nil, err
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
	return (&HaSqliteDBStore{db: db}).init()
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

// init 初始化表结构
func (s *HaSqliteDBStore) init() (*HaSqliteDBStore, error) {
	input := HaSqliteDBStoreDBItem{}
	vType := reflect.TypeOf(input)
	var fields []string
	var indexSqlArray []*struct {
		sqlText   string
		name      string
		tableName string
	}
	for i := 0; i < vType.NumField(); i++ {
		fieldValue := vType.Field(i).Tag.Get("sqlite3_field")
		if fieldValue == "" || fieldValue == "-" {
			continue
		}
		sqliteType := getSqliteTypeFromKindName(vType.Field(i).Type.String())
		if sqliteType == "" {
			continue
		}
		fieldSql := fmt.Sprintf(`"%s" %s`, fieldValue, sqliteType)
		fieldDefault := vType.Field(i).Tag.Get("sqlite3_default")
		if fieldValue != "-" && fieldDefault != "" {
			fieldSql += " " + fieldDefault
		}
		fields = append(fields, fieldSql)
		fieldIndex := vType.Field(i).Tag.Get("sqlite3_index")
		if fieldValue == "" || fieldValue == "-" {
			continue
		}
		indexSql := fmt.Sprintf("CREATE %s \"%s_%s\" ON \"%s\" (\"%s\");", fieldIndex, tableName, fieldValue, tableName, fieldValue)
		indexSqlArray = append(indexSqlArray, &struct {
			sqlText   string
			name      string
			tableName string
		}{sqlText: indexSql, name: fmt.Sprintf("%s_%s", tableName, fieldValue), tableName: tableName})
	}
	sqlText := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", tableName, strings.Join(fields, ","))
	_, err := s.db.Exec(sqlText)
	if err != nil {
		return nil, err
	}
	var indexCount int64
	for _, indexSql := range indexSqlArray {
		if err := s.db.QueryRow("select count(*) from sqlite_master where type = ? and name = ? and tbl_name = ?", "index", indexSql.name, indexSql.tableName).Scan(&indexCount); err != nil {
			if err == sql.ErrNoRows {
				_, err := s.db.Exec(indexSql.sqlText)
				if err != nil {
					return nil, err
				}
				continue
			}
			return nil, err
		}
		if indexCount != 0 {
			continue
		}
		_, err := s.db.Exec(indexSql.sqlText)
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

// GetDBIdByPath 通过路径获取数据库 id
func (s *HaSqliteDBStore) GetDBIdByPath(path string) (int64, bool, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	var id int64
	if err := s.db.QueryRow("select id from ha_sqlite where path = ? order by id asc limit 1 ", path).Scan(&id); err != nil {
		if err == sql.ErrNoRows {
			return 0, false, nil
		}
		return id, false, fmt.Errorf("GetDBIdByPath(%s) error:%v", path, err)
	}
	return id, true, nil
}

// GetDBPathById 通过 id 获取库文件路径
func (s *HaSqliteDBStore) GetDBPathById(id int64) (path string, err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	err = s.db.QueryRow("select path from ha_sqlite where id = ? limit 1 ", id).Scan(&path)
	return
}

// GetDBUpdateTimeById 通过 id 获取库最后一次更新时间
func (s *HaSqliteDBStore) GetDBUpdateTimeById(id int64) (updateTime int64, err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	err = s.db.QueryRow("select update_time from ha_sqlite where id = ? limit 1 ", id).Scan(&updateTime)
	return
}

// CreateDBByPath 通过路径创建数据库并返回 id
func (s *HaSqliteDBStore) CreateDBByPath(path string) (int64, error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	unix := time.Now().UnixMilli()
	r, err := s.db.Exec("INSERT INTO ha_sqlite(path,db_version,snapshot_version,create_time,update_time) VALUES(?,?,?,?,?)", path, 0, 0, unix, unix)
	if err != nil {
		return 0, fmt.Errorf("CreateDBByPath exec error:%v", err)
	}
	insertId, err := r.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("CreateDBByPath get LastInsertId error:%v", err)
	}
	return insertId, nil
}

// RefDBUpdateTimeById 通过 id 刷新更新时间
func (s *HaSqliteDBStore) RefDBUpdateTimeById(id int64) error {
	//s.mtx.Lock()
	//defer s.mtx.Unlock()
	//unix := time.Now().UnixMilli()
	//_, err := s.db.Exec("UPDATE ha_sqlite SET update_time = ? WHERE id = ?", unix, id)
	//if err != nil {
	//	return fmt.Errorf("RefDBUpdateTimeById exec error:%v", err)
	//}
	return nil
}

// GetDBInfo 获取数据库信息
func (s *HaSqliteDBStore) GetDBInfo(request *proto.DBInfoRequest) (resp *proto.DBInfoResponse, err error) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	query := "select id,path,version,create_time,update_time from ha_sqlite where id = ? or path = ? limit 1 "
	err = s.db.QueryRow(query, request.GetDbId(), request.GetDsn()).Scan(&resp.DbId, &resp.Dsn, &resp.Version, &resp.CreateTime, &resp.UpdateTime)
	return
}
