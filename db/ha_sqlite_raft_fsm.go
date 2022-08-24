package db

import (
	"github.com/hashicorp/raft"
	"io"
	"log"
	"sync"
)

// HaSqliteRaftFSM Raft 生命周期相关接口实现
type HaSqliteRaftFSM struct {
	mtx sync.RWMutex
}

//var _ raft.FSM = &HaSqliteRaftFSM{}

func NewHaSqliteRaftFSM() *HaSqliteRaftFSM {
	return &HaSqliteRaftFSM{}
}

func (h *HaSqliteRaftFSM) Apply(l *raft.Log) interface{} {
	log.Println("HaSqliteRaftFSM.Apply")
	h.mtx.Lock()
	defer h.mtx.Unlock()
	return nil
}

func (h *HaSqliteRaftFSM) Snapshot() (raft.FSMSnapshot, error) {
	log.Println("HaSqliteRaftFSM.Snapshot")
	// Make sure that any future calls to f.Apply() don't change the snapshot.
	return &HaSqliteSnapshot{}, nil
}

func (h *HaSqliteRaftFSM) Restore(r io.ReadCloser) error {
	log.Println("HaSqliteRaftFSM.Restore")

	return nil
}
