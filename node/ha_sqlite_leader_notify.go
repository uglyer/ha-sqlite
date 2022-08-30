package node

import (
	"github.com/hashicorp/raft"
	"sync"
)

type HaSqliteLeaderNotify struct {
	raft *raft.Raft
	mtx  sync.Mutex
	//notifyList s
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

		}
	}()
}

func (n *HaSqliteLeaderNotify) hasLeader() bool {
	_, address := n.raft.LeaderWithID()
	return address != ""
}

func (n *HaSqliteLeaderNotify) WaitHasLeader() {

}
