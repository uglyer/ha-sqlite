package node

import (
	"context"
	"fmt"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	"github.com/hashicorp/raft"
	"github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"time"
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
	fsm := db.NewHaSqliteRaftFSM()
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
	if config.RaftAdmin {
		raftadmin.Register(s, r)
	}
	proto.RegisterHaSqliteInternalServer(s, c)
	proto.RegisterDBServer(s, c)
	if config.JoinAddress != "" {
		needJoin := c.needRequestJoin()
		if !needJoin {
			log.Printf("already join skip")
			return c, nil
		}
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

// IsLeader 当前节点是否为 leader
func (ctx *HaSqliteContext) needRequestJoin() bool {
	config := ctx.Raft.GetConfiguration()
	for _, v := range config.Configuration().Servers {
		if v.ID == raft.ServerID(ctx.Config.RaftId) && v.Address == raft.ServerAddress(ctx.Config.Address) {
			// 如果完全一致, 加入节点
			return false
		}
	}
	return true
}

func (ctx *HaSqliteContext) Join(c context.Context, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	if !ctx.IsLeader() {
		// 如果不是 leader，转发到 leader 执行
		leaderAddress, _ := ctx.Raft.LeaderWithID()
		return ctx.CallRemoteJoin(string(leaderAddress), req)
	}
	config := ctx.Raft.GetConfiguration()
	for _, v := range config.Configuration().Servers {
		if v.ID == raft.ServerID(req.Id) && v.Address == raft.ServerAddress(req.Address) {
			// 如果完全一致, 加入节点
			return &proto.JoinResponse{
				Code:    proto.ResultCode_SUCCESS,
				Message: "same node info",
				Index:   ctx.Raft.LastIndex(),
			}, nil
		} else if v.ID == raft.ServerID(req.Id) && v.Address != raft.ServerAddress(req.Address) {
			// 节点id一致, 但地址不一致, 移除原有节点
			future := ctx.Raft.RemoveServer(raft.ServerID(req.Id), req.PreviousIndex, timeout(c))
			err := future.Error()
			if err != nil {
				return &proto.JoinResponse{
					Code:    proto.ResultCode_UNKNOWN_FAILED,
					Message: err.Error(),
					Index:   ctx.Raft.LastIndex(),
				}, fmt.Errorf("Join error: %v", err)
			}
			break
		}
	}
	future := ctx.Raft.AddVoter(raft.ServerID(req.Id), raft.ServerAddress(req.Address), req.PreviousIndex, timeout(c))
	err := future.Error()
	if err != nil {
		return &proto.JoinResponse{
			Code:    proto.ResultCode_UNKNOWN_FAILED,
			Message: err.Error(),
			Index:   ctx.Raft.LastIndex(),
		}, fmt.Errorf("Join error: %v", err)
	}
	return &proto.JoinResponse{
		Code:    proto.ResultCode_SUCCESS,
		Message: "success",
		Index:   ctx.Raft.LastIndex(),
	}, nil
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

func timeout(ctx context.Context) time.Duration {
	if dl, ok := ctx.Deadline(); ok {
		return dl.Sub(time.Now())
	}
	return 0
}

// Open 打开数据库
func (ctx *HaSqliteContext) Open(c context.Context, req *proto.OpenRequest) (*proto.OpenResponse, error) {
	return nil, fmt.Errorf("todo db open")
}

// Exec 执行数据库命令
func (ctx *HaSqliteContext) Exec(c context.Context, req *proto.ExecRequest) (*proto.ExecResponse, error) {
	return nil, fmt.Errorf("todo db Exec")
}

// Query 查询记录
func (ctx *HaSqliteContext) Query(c context.Context, req *proto.QueryRequest) (*proto.QueryResponse, error) {
	return nil, fmt.Errorf("todo db Query")
}
