package main

import (
	"fmt"
	"github.com/uglyer/ha-sqlite/node"
	"log"
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
	log.Println(config)
	ctx, err := node.NewHaSqliteContext(config)
	if err != nil {
		log.Fatalf("failed to start HaSqliteContext: %v", err)
	}
	defer ctx.Sock.Close()
	if err := ctx.GrpcServer.Serve(ctx.Sock); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
