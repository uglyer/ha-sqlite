package main

import (
	"flag"
	"fmt"
)

// ParseFlags parses the command line, and returns the configuration.
func ParseFlags() (*HaSqlitedConfig, error) {
	if flag.Parsed() {
		return nil, fmt.Errorf("command-line flags already parsed")
	}
	config := &HaSqlitedConfig{}
	flag.StringVar(&config.Address, "address", "localhost:30051", "TCP host+port for this node")
	flag.StringVar(&config.RaftId, "raft_id", "", "Node id used by Raft")

	flag.StringVar(&config.DataPath, "data_path", "data/", "Raft data dir")
	flag.BoolVar(&config.RaftBootstrap, "raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
	flag.StringVar(&config.JoinAddress, "join_address", "", "auto join cluster")
	flag.Parse()
	return config, nil
}
