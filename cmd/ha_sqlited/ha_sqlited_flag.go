package main

import (
	"flag"
	"fmt"
	"github.com/uglyer/ha-sqlite/node"
	"net"
)

// ParseFlags parses the command line, and returns the configuration.
func ParseFlags() (*node.HaSqliteConfig, error) {
	if flag.Parsed() {
		return nil, fmt.Errorf("command-line flags already parsed")
	}
	config := &node.HaSqliteConfig{}
	flag.StringVar(&config.Address, "address", "localhost:30051", "TCP host+port for this node")
	flag.StringVar(&config.RaftId, "raft_id", "", "Node id used by Raft")

	flag.StringVar(&config.DataPath, "data_path", "data/", "Raft data dir")
	flag.BoolVar(&config.RaftBootstrap, "raft_bootstrap", false, "Whether to bootstrap the Raft cluster")
	flag.BoolVar(&config.RaftAdmin, "raft_admin", false, "register raftAdmin grpc")
	flag.StringVar(&config.JoinAddress, "join_address", "", "auto join cluster")
	flag.Parse()
	if config.RaftId == "" {
		return nil, fmt.Errorf("flag --raft_id is required")
	}
	if config.RaftBootstrap && config.JoinAddress != "" {
		return nil, fmt.Errorf("--raft_bootstrap 与 --join_address 为互斥项")
	}
	_, port, err := net.SplitHostPort(config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse local address (%q): %v", config.Address, err)
	}
	config.LocalPort = port
	return config, nil
}
