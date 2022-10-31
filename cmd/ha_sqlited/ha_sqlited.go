package main

import (
	"fmt"
	hadb "github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

const name = `ha-sqlited`

func init() {
	log.SetFlags(log.LstdFlags)
	log.SetOutput(os.Stderr)
	log.SetPrefix(fmt.Sprintf("[%s] ", name))
}

func main() {
	config, err := ParseFlags()
	if err != nil {
		log.Fatalf("failed to parse command-line flags: %s", err.Error())
	}
	_, port, err := net.SplitHostPort(config.HaSqlite.Address)
	if err != nil {
		log.Fatalf("failed to parse local address (%q): %v", config.HaSqlite.Address, err)
	}
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatalf("failed to listen (%s): %v", port, err)
	}
	store, err := hadb.NewHaSqliteDBManager()
	if err != nil {
		log.Fatalf("failed to create db manager: %v", err)
	}
	s := grpc.NewServer()
	proto.RegisterDBServer(s, store)
	go func() {
		log.Printf("start server in (%s)", config.HaSqlite.Address)
	}()
	err = s.Serve(sock)
	if err != nil {
		log.Fatalf("failed to start serve (%q): %v", config.HaSqlite.Address, err)
	}
}
