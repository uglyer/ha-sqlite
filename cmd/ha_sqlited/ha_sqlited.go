package main

import (
	"fmt"
	hadb "github.com/uglyer/ha-sqlite/db"
	"github.com/uglyer/ha-sqlite/log"
	"github.com/uglyer/ha-sqlite/proto"
	"github.com/uglyer/ha-sqlite/s3"
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
	var store *hadb.HaSqliteDBManager
	if config.S3.Enabled {
		s3Store, err := s3.NewS3Client(config.S3)
		if err != nil {
			log.Fatal(fmt.Sprintf("failed to create s3 client: %v", err))
		}
		store, err = hadb.NewHaSqliteDBManagerWithConfigAndS3(config.HaSqlite, s3Store)
	} else {
		store, err = hadb.NewHaSqliteDBManagerWithConfig(config.HaSqlite)
	}
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
