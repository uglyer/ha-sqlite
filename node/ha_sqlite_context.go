package node

import (
	"context"
	"fmt"
	"github.com/Jille/raft-grpc-leader-rpc/leaderhealth"
	"github.com/Jille/raftadmin"
	"github.com/hashicorp/raft"
	"github.com/shimingyah/pool"
	"github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"log"
	"net"
	"sync"
	"time"
)

type HaSqliteContext struct {
	// Config 配置参数
	Ctx        context.Context
	Config     *HaSqliteConfig
	fsm        *db.HaSqliteRaftFSM
	Raft       *raft.Raft
	Sock       net.Listener
	GrpcServer *grpc.Server
	poolMtx    sync.RWMutex
	poolMap    map[string]pool.Pool
}

// StartHaSqliteBlockNonBlocking 启动服务非阻运行
func StartHaSqliteBlockNonBlocking(config *HaSqliteConfig) (*HaSqliteContext, error) {
	ctx, err := NewHaSqliteContext(config)
	if err != nil {
		return nil, err
	}
	go func() {
		defer ctx.Sock.Close()
		if err := ctx.GrpcServer.Serve(ctx.Sock); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()
	return ctx, err
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
		GrpcServer: s,
		poolMap:    make(map[string]pool.Pool),
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
		resp, err := c.callRemoteJoin(config.JoinAddress, &proto.JoinRequest{
			Id:            c.Config.RaftId,
			Address:       c.Config.Address,
			PreviousIndex: c.Raft.LastIndex(),
		})
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

// getGrpcConn 获取rpc连接池
func (ctx *HaSqliteContext) getRPCPollConn(addr string) (pool.Conn, error) {
	ctx.poolMtx.Lock()
	defer ctx.poolMtx.Unlock()
	if p, ok := ctx.poolMap[addr]; ok {
		return p.Get()
	}
	p, err := pool.New(addr, pool.DefaultOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to new pool: %v", err)
	}
	ctx.poolMap[addr] = p
	return p.Get()
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
		return ctx.callRemoteJoin(string(leaderAddress), req)
	}
	config := ctx.Raft.GetConfiguration()
	id := raft.ServerID(req.Id)
	addr := raft.ServerAddress(req.Address)
	for _, v := range config.Configuration().Servers {
		if v.ID == id && v.Address == addr {
			// 如果完全一致, 曾经加入过节点
			return nil, fmt.Errorf("cluster has same id(%s) and address(%s)", req.Id, req.Address)
		} else if v.ID == id && v.Address != addr {
			// 节点id一致, 但地址不一致, 移除原有节点
			if err := ctx.Raft.RemoveServer(id, req.PreviousIndex, timeout(c)).Error(); err != nil {
				return nil, fmt.Errorf("Join error: %v", err)
			}
			break
		}
	}
	future := ctx.Raft.AddVoter(id, addr, req.PreviousIndex, timeout(c))
	err := future.Error()
	if err != nil {
		return nil, fmt.Errorf("Join error: %v", err)
	}
	return &proto.JoinResponse{Index: ctx.Raft.LastIndex()}, nil
}

// callRemoteJoin 发起远程调用加入节点
func (ctx *HaSqliteContext) callRemoteJoin(remoteAddress string, req *proto.JoinRequest) (*proto.JoinResponse, error) {
	conn, err := ctx.getRPCPollConn(remoteAddress)
	if err != nil {
		return nil, fmt.Errorf("CallRemoteJoin open conn error: %v", err)
	}
	defer conn.Close()
	client := proto.NewHaSqliteInternalClient(conn.Value())
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
