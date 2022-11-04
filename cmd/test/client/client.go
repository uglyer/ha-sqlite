package main

import (
	"context"
	"fmt"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"io"
	"log"
	"sync"
	"time"
)

func main() {
	var o grpc.DialOption = grpc.EmptyDialOption{}
	conn, err := grpc.Dial("127.0.0.1:30333", grpc.WithInsecure(), grpc.WithBlock(), o)
	if err != nil {
		log.Fatalf("NewHaSqliteConn open conn error#1: %v", err)
	}
	go func() {
		time.Sleep(time.Second * 3)
		fmt.Println("CloseSend")
		conn.Close()
	}()
	client := proto.NewStreamTestClient(conn)
	pingStreamTest(client)
}

func pingStreamTest(client proto.StreamTestClient) {
	ctx := context.Background()
	// 2.调用获取stream
	stream, err := client.PingStream(ctx, &proto.StreamPingRequest{Timestamp: time.Now().UnixMilli()})
	if err != nil {
		log.Fatalf("could not echo: %v", err)
	}
	// 3. for循环获取服务端推送的消息
	go func() {
		time.Sleep(time.Second * 4)
		fmt.Println("CloseSend")

		//stream.
		//fmt.Printf("CloseSend:%v", ok)
	}()
	for {
		// 通过 Recv() 不断获取服务端send()推送的消息
		resp, err := stream.Recv()
		// 4. err==io.EOF则表示服务端关闭stream了 退出
		if err == io.EOF {
			log.Println("server closed")
			break
		}
		if err != nil {
			log.Printf("Recv error:%v", err)
			break
		}
		log.Printf("Recv data:%v", resp.String())
	}
}

func pingDoubleStreamTest(client proto.StreamTestClient) {
	var wg sync.WaitGroup
	// 2. 调用方法获取stream
	stream, err := client.PingDoubleStream(context.Background())
	if err != nil {
		panic(err)
	}
	// 3.开两个goroutine 分别用于Recv()和Send()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			req, err := stream.Recv()
			if err == io.EOF {
				fmt.Println("Server Closed")
				break
			}
			if err != nil {
				continue
			}
			fmt.Printf("Recv Data:%v \n", req.String())
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()

		for i := 0; i < 2; i++ {
			err := stream.Send(&proto.StreamPingRequest{Timestamp: time.Now().UnixMilli()})
			if err != nil {
				log.Printf("send error:%v\n", err)
			}
			time.Sleep(time.Second)
		}
		// 4. 发送完毕关闭stream
		err := stream.CloseSend()
		if err != nil {
			log.Printf("Send error:%v\n", err)
			return
		}
	}()
	wg.Wait()
}
