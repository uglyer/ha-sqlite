package main

import (
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type server struct {
}

func (*server) PingDoubleStream(stream proto.StreamTest_PingDoubleStreamServer) error {
	var (
		waitGroup sync.WaitGroup
		msgCh     = make(chan string)
	)
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		for v := range msgCh {
			fmt.Printf("chan recv:%v", v)
			err := stream.Send(&proto.StreamPingResponse{Timestamp: time.Now().UnixMilli()})
			if err != nil {
				fmt.Println("Send error:", err)
				continue
			}
		}
		fmt.Printf("chan closed")
	}()

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("recv error:%v", err)
			}
			fmt.Printf("Recved :%v \n", req.Timestamp)
			msgCh <- req.String()
		}
		close(msgCh)
	}()
	waitGroup.Wait()
	// 返回nil表示已经完成响应
	return nil
}
func (*server) PingStream(req *proto.StreamPingRequest, stream proto.StreamTest_PingStreamServer) error {
	log.Printf("Recved %v", req.String())
	for i := 0; i < 1000; i++ {
		// 通过 send 方法不断推送数据
		err := stream.Send(&proto.StreamPingResponse{Timestamp: time.Now().UnixMilli()})
		if err != nil {
			log.Printf("Send error:%v", err)
			return err
		}
		time.Sleep(time.Second)
	}
	// 返回nil表示已经完成响应
	return nil
}

func main() {
	port := 30333
	sock, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to listen (%d): %v", port, err))
	}
	ser := &server{}
	s := grpc.NewServer()
	proto.RegisterStreamTestServer(s, ser)
	s.Serve(sock)
}
