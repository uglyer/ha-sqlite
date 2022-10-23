package node

import "path"

type HaSqliteConfig struct {
	// Address TCP host+port for this node
	Address string `yaml:"address"`
	// LocalPort TCP port for this node
	LocalPort string `yaml:"local_port"`
	// RaftId Node id used by Raft
	RaftId string `yaml:"raft_id"`
	// DataPath is path to node data. Always set.
	DataPath string `yaml:"data_path"`
	// RaftBootstrap Whether to bootstrap the Raft cluster
	RaftBootstrap bool `yaml:"raft_bootstrap"`
	// RaftAdmin register raftAdmin grpc
	RaftAdmin bool `yaml:"raft_admin"`
	// JoinAddress auto join cluster
	JoinAddress string `yaml:"join_address"`
}

func (c *HaSqliteConfig) NodeDataPath() string {
	return path.Join(c.DataPath, c.RaftId)
}
