package node

import (
	"context"
	"fmt"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/hashicorp/raft"
	"github.com/uglyer/ha-sqlite/db"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

type HaSqliteContext struct {
	// Config 配置参数
	Config *HaSqliteConfig
	Node   *db.HaSqliteNode
	Raft   *raft.Raft
}

func NewHaSqliteContext(config *HaSqliteConfig) (*HaSqliteContext, error) {
	ctx := context.Background()
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", config.LocalPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer sock.Close()
	node := &db.HaSqliteNode{}
	r, tm, err := NewRaft(ctx, config, node)
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"Example"})
	reflection.Register(s)
	if err := s.Serve(sock); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	return &HaSqliteContext{
		Config: config,
		Node:   node,
		Raft:   r,
	}, nil
}
