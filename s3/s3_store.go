package s3

type S3Store interface {
	//Snapshot 快照操作, remotePath 不包含 PrefixPath
	Snapshot(dbPath string, remotePath string) error
	//Restore 恢复操作, remotePath 不包含 PrefixPath
	Restore(dbPath string, remotePath string) error
}
