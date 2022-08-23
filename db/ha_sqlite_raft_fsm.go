package db

import (
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

// HaSqliteRaftFSM Raft 生命周期相关接口实现
type HaSqliteRaftFSM struct {
	mtx sync.RWMutex
}

//var _ raft.FSM = &HaSqliteRaftFSM{}

func (h *HaSqliteRaftFSM) Apply(l *raft.Log) interface{} {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	return nil
}

func (h *HaSqliteRaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	// Make sure that any future calls to f.Apply() don't change the snapshot.
	return &HaSqliteSnapshot{}, nil
}

func (h *HaSqliteRaftFSM) Restore(r io.ReadCloser) error {

	return nil
}
