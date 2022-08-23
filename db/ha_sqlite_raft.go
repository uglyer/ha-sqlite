package db

import (
	"github.com/hashicorp/raft"
	"io"
	"sync"
)

// HaSqliteNode 高可用sqlite节点.
type HaSqliteNode struct {
	mtx sync.RWMutex
}

//var _ raft.FSM = &HaSqliteNode{}

func (h *HaSqliteNode) Apply(l *raft.Log) interface{} {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	return nil
}

func (h *HaSqliteNode) Snapshot() (raft.FSMSnapshot, error) {
	// Make sure that any future calls to f.Apply() don't change the snapshot.
	return &HaSqliteSnapshot{}, nil
}

func (h *HaSqliteNode) Restore(r io.ReadCloser) error {

	return nil
}
