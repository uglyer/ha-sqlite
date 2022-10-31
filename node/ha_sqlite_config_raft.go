package node

import "path"

type HaSqliteRaftConfig struct {
	// Address TCP host+port for this node
	Address string `mapstructure:"address" yaml:"address"`
	// LocalPort TCP port for this node
	LocalPort string `mapstructure:"local-port" yaml:"local-port"`
	// RaftId Node id used by Raft
	RaftId string `mapstructure:"raft-id" yaml:"raft-id"`
	// DataPath is path to node data. Always set.
	DataPath string `mapstructure:"data-path" yaml:"data-path"`
	// RaftBootstrap Whether to bootstrap the Raft cluster
	RaftBootstrap bool `mapstructure:"raft-bootstrap" yaml:"raft-bootstrap"`
	// RaftAdmin register raftAdmin grpc
	RaftAdmin bool `mapstructure:"raft-admin" yaml:"raft-admin"`
	// JoinAddress auto join cluster
	JoinAddress string `mapstructure:"join-address" yaml:"join-address"`
}

func (c *HaSqliteRaftConfig) NodeDataPath() string {
	return path.Join(c.DataPath, c.RaftId)
}
