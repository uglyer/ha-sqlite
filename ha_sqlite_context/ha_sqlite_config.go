package ha_sqlite_context

type HaSqliteConfig struct {
	// Address TCP host+port for this node
	Address string
	// LocalPort TCP port for this node
	LocalPort string
	// RaftId Node id used by Raft
	RaftId string
	// DataPath is path to node data. Always set.
	DataPath string
	// RaftBootstrap Whether to bootstrap the Raft cluster
	RaftBootstrap bool
	// JoinAddress auto join cluster
	JoinAddress string
}
