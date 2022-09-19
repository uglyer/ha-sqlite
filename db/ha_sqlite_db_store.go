package db

import "database/sql"

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
