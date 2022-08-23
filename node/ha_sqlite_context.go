package node

import (
	"context"
	"fmt"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/hashicorp/raft"
	"github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
)

type HaSqliteContext struct {
	// Config 配置参数
	Ctx        context.Context
	Config     *HaSqliteConfig
	fsm        *db.HaSqliteRaftFSM
	Raft       *raft.Raft
	Sock       net.Listener
	GPpcServer *grpc.Server
}

func NewHaSqliteContext(config *HaSqliteConfig) (*HaSqliteContext, error) {
	ctx := context.Background()
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", config.LocalPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	fsm := &db.HaSqliteRaftFSM{}
	r, tm, err := NewRaft(ctx, config, fsm)
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	c := &HaSqliteContext{
		Ctx:        ctx,
		Config:     config,
		fsm:        fsm,
		Raft:       r,
		Sock:       sock,
		GPpcServer: s,
	}
	tm.Register(s)
	leaderhealth.Setup(r, s, []string{"HaSqliteInternal"})
	reflection.Register(s)
	proto.RegisterHaSqliteInternalServer(s, c)
	if config.JoinAddress != "" {
		log.Printf("start join %v", config.JoinAddress)
		resp, err := c.CallRemoteJoinWithSelf(config.JoinAddress)
		if err != nil {
			c.Raft.Shutdown()
			return nil, fmt.Errorf("failed to join: %v,%v", resp, err)
		}
		log.Println(resp)
	} else {
		log.Printf("with out JoinAddress, skip join")
	}
	return c, nil
}

// IsLeader 当前节点是否为 leader
func (ctx *HaSqliteContext) IsLeader() bool {
	return ctx.Raft.State() == raft.Leader
}

func (ctx *HaSqliteContext) Join(c context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	if !ctx.IsLeader() {
		// 如果不是 leader，转发到 leader 执行
		leaderAddress, _ := ctx.Raft.LeaderWithID()
		return ctx.CallRemoteJoin(string(leaderAddress), req)
	}
	return &proto.JoinResponse{
		Code:    proto.ResultCode_NOT_A_LEADER,
		Message: proto.ResultCode_NOT_A_LEADER.String(),
		Index:   ctx.Raft.LastIndex(),
	}, fmt.Errorf("TODO impl Join")
}

// CallRemoteJoinWithSelf 使用自身参数构建发起远程调用加入节点
func (ctx *HaSqliteContext) CallRemoteJoinWithSelf(remoteAddress string) (*proto.JoinResponse, error) {
	req := &proto.JoinRequest{
		Id:            ctx.Config.RaftId,
		Address:       ctx.Config.Address,
		PreviousIndex: ctx.Raft.LastIndex(),
	}
	return ctx.CallRemoteJoin(remoteAddress, req)
}

// CallRemoteJoin 发起远程调用加入节点
func (ctx *HaSqliteContext) CallRemoteJoin(remoteAddress string, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	var o grpc.DialOption = grpc.EmptyDialOption{}
	conn, err := grpc.Dial(remoteAddress, grpc.WithInsecure(), grpc.WithBlock(), o)
	if err != nil {
		return nil, fmt.Errorf("CallRemoteJoin open conn error: %v", err)
	}
	defer conn.Close()
	client := proto.NewHaSqliteInternalClient(conn)
	return client.Join(ctx.Ctx, req)
}
