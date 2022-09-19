package db

import (
	"database/sql"
)

// HaSqliteDBStore 使用系统信息管理 db(memory or disk)
// 用于存放dsn、dbId、本地文件路径、拉取状态(本地、S3远端)、版本号、最后一次更新时间、最后一次查询时间、快照版本 等信息
type HaSqliteDBStore struct {
	db *sql.DB
}

type HaSqliteDBStoreDBItem struct {
	Id              string `sqlite:"id"`
	Path            string `sqlite:"path"`
	DBVersion       uint64 `sqlite:"db_version"`
	SnapshotVersion uint64 `sqlite:"snapshot_version"`
	CreateTime      int64  `sqlite:"create_time"`
	UpdateTime      int64  `sqlite:"update_time"`
}

const dbName = "ha_sqlite"

// NewHaSqliteDBStore 创建新的数仓，会自动创建数据库
func NewHaSqliteDBStore() (*HaSqliteDBStore, error) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		return nil, err
	}
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	_, err = db.Exec("create table ? (id integer not null primary key, path text, db_version text,snapshot_version text,create_time integer,update_time integer)", dbName)
	if err != nil {
		return nil, err
	}
	return &HaSqliteDBStore{db: db}, nil
}
