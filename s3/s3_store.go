package s3

import "github.com/minio/minio-go/v6"

type S3Store interface {
	//Snapshot 快照操作, remotePath 不包含 PrefixPath
	Snapshot(dbPath string, remotePath string) error
	//Restore 恢复操作, remotePath 不包含 PrefixPath
	Restore(dbPath string, remotePath string) error
	//StatObject 获取远端状态信息, remotePath 不包含 PrefixPath
	StatObject(remotePath string) (minio.ObjectInfo, error)
}
