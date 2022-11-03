package main

import (
	"fmt"
	hadb "github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/log"
	"github.com/uglyer/ha-sqlite/proto"
	"google.golang.org/grpc"
	"net"
)

const name = `ha-sqlited`

func main() {
	config, err := ParseFlags()
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to parse command-line flags: %s", err.Error()))
	}
	log.SetDefaultLogMode(config.Log)
	_, port, err := net.SplitHostPort(config.HaSqlite.Address)
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to parse local address (%q): %v", config.HaSqlite.Address, err))
	}
	sock, err := net.Listen("tcp", fmt.Sprintf(":%s", port))
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to listen (%s): %v", port, err))
	}
	store, err := hadb.NewHaSqliteDBManagerWithConfig(config.HaSqlite)
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to create db manager: %v", err))
	}
	s := grpc.NewServer()
	proto.RegisterDBServer(s, store)
	go func() {
		log.Info(fmt.Sprintf("start server in (%s)", config.HaSqlite.Address))
	}()
	err = s.Serve(sock)
	if err != nil {
		log.Fatal(fmt.Sprintf("failed to start serve (%q): %v", config.HaSqlite.Address, err))
	}
}
