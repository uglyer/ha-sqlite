package main

type HaSqlitedConfig struct {
	// Address TCP host+port for this node
	Address string
	// RaftId Node id used by Raft
	RaftId string
	// DataPath is path to node data. Always set.
	DataPath string
	// RaftBootstrap Whether to bootstrap the Raft cluster
	RaftBootstrap bool
	// JoinAddress auto join cluster
	JoinAddress string
}
