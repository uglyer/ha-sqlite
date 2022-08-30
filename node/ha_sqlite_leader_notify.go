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
}

func NewHaSqliteLeaderNotify(raft *raft.Raft) *HaSqliteLeaderNotify {
	notify := &HaSqliteLeaderNotify{
		raft:       raft,
		notifyList: list.New(),
	}
	notify.observeLeader()
	return notify
}

// TODO 实现等待成功选举领导接口

func (n *HaSqliteLeaderNotify) observeLeader() {
	ch := make(chan raft.Observation, 1)
	n.raft.RegisterObserver(raft.NewObserver(ch, true, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	}))
	go func() {
		for range ch {
			if !n.hasLeader() {
				continue
			}
			n.mtx.Lock()
			defer n.mtx.Unlock()
			for {
				if n.notifyList.Len() == 0 {
					continue
				}
				it := n.notifyList.Front()
				n.notifyList.Remove(it)
				it.Value.(chan struct{}) <- struct{}{}
			}
		}
	}()
}

func (n *HaSqliteLeaderNotify) hasLeader() bool {
	_, address := n.raft.LeaderWithID()
	return address != ""
}

func (n *HaSqliteLeaderNotify) WaitHasLeader() {
	n.mtx.Lock()
	if n.hasLeader() {
		n.mtx.Unlock()
		return
	}
	ch := make(chan struct{}, 1)
	defer close(ch)
	n.notifyList.PushBack(ch)
	n.mtx.Unlock()
	<-ch
}
