package db

import (
	"github.com/hashicorp/raft"
	"log"
)

type HaSqliteSnapshot struct {
}

// Persist TODO 快照实现，快照时将本地数据库文件归档至s3或磁盘归档并移除不活跃的本地文件(将会在下次活跃时从归档中拉取)，将系统库克隆一并归档
func (s *HaSqliteSnapshot) Persist(sink raft.SnapshotSink) error {

	log.Println("HaSqliteRaftFSM.Persist")
	return sink.Close()
}

func (s *HaSqliteSnapshot) Release() {
	log.Println("HaSqliteRaftFSM.Release")
}
