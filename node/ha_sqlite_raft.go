package node

import (
	"context"
	"fmt"
	transport "github.com/Jille/raft-grpc-transport"
	"github.com/hashicorp/raft"
	boltdb "github.com/hashicorp/raft-boltdb"
	"github.com/uglyer/ha-sqlite/tool"
	"google.golang.org/grpc"
	"log"
	"os"
	"path/filepath"
)

func NewRaft(ctx context.Context, config *HaSqliteConfig, fsm raft.FSM) (*raft.Raft, *transport.Manager, error) {
	c := raft.DefaultConfig()
	c.LocalID = raft.ServerID(config.RaftId)

	baseDir := filepath.Join(config.DataPath, config.RaftId)

	err := tool.FSCreateMultiDir(baseDir)
	if err != nil {
		return nil, nil, fmt.Errorf(`create data dir error (%q): %v`, baseDir, err)
	}

	ldb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "logs.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "logs.dat"), err)
	}

	sdb, err := boltdb.NewBoltStore(filepath.Join(baseDir, "stable.dat"))
	if err != nil {
		return nil, nil, fmt.Errorf(`boltdb.NewBoltStore(%q): %v`, filepath.Join(baseDir, "stable.dat"), err)
	}

	fss, err := raft.NewFileSnapshotStore(baseDir, 3, os.Stderr)
	if err != nil {
		return nil, nil, fmt.Errorf(`raft.NewFileSnapshotStore(%q, ...): %v`, baseDir, err)
	}

	tm := transport.New(raft.ServerAddress(config.Address), []grpc.DialOption{grpc.WithInsecure()})

	r, err := raft.NewRaft(c, fsm, ldb, sdb, fss, tm.Transport())
	if err != nil {
		return nil, nil, fmt.Errorf("raft.NewRaft: %v", err)
	}

	if config.RaftBootstrap {
		if r.LastIndex() == 0 {
			cfg := raft.Configuration{
				Servers: []raft.Server{
					{
						Suffrage: raft.Voter,
						ID:       raft.ServerID(config.RaftId),
						Address:  raft.ServerAddress(config.Address),
					},
				},
			}
			f := r.BootstrapCluster(cfg)
			if err := f.Error(); err != nil {
				// 跳过错误
				fmt.Errorf("raft.Raft.BootstrapCluster: %v", err)
			}
		} else {
			log.Printf("last index is %v, skip bootstrap", r.LastIndex())
		}
	}

	return r, tm, nil
}
