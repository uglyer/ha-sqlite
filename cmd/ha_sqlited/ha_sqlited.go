package main

import (
	"fmt"
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
}
