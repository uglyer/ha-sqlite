package db

import (
	"github.com/hashicorp/raft"
)

type HaSqliteSnapshot struct {
}

func (s *HaSqliteSnapshot) Persist(sink raft.SnapshotSink) error {

	return sink.Close()
}

func (s *HaSqliteSnapshot) Release() {
}
