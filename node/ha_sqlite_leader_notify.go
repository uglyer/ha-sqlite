package node

import (
	"container/list"
	"github.com/hashicorp/raft"
	"sync"
)

type HaSqliteLeaderNotify struct {
	raft       *raft.Raft
	mtx        sync.Mutex
	notifyList *list.List
	hasLeader  bool
}

func NewHaSqliteLeaderNotify(raft *raft.Raft) *HaSqliteLeaderNotify {
	notify := &HaSqliteLeaderNotify{
		raft:       raft,
		notifyList: list.New(),
		hasLeader:  false,
	}
	notify.observeLeader()
	return notify
}

func (n *HaSqliteLeaderNotify) observeLeader() {
	ch := make(chan raft.Observation, 1)
	n.raft.RegisterObserver(raft.NewObserver(ch, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		n.mtx.Lock()
		n.hasLeader = ok
		n.mtx.Unlock()
		return ok
	}))
	go func() {
		for o := range ch {
			_, ok := o.Data.(raft.LeaderObservation)
			if !ok {
				return
			}
			n.mtx.Lock()
			for {
				if n.notifyList.Len() == 0 {
					break
				}
				it := n.notifyList.Front()
				n.notifyList.Remove(it)
				it.Value.(chan struct{}) <- struct{}{}
			}
			n.mtx.Unlock()
		}
	}()
}

func (n *HaSqliteLeaderNotify) WaitHasLeader() {
	n.mtx.Lock()
	if n.hasLeader {
		n.mtx.Unlock()
		return
	}
	ch := make(chan struct{}, 1)
	defer close(ch)
	n.notifyList.PushBack(ch)
	n.mtx.Unlock()
	<-ch
}
