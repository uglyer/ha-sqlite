package main

import (
	"fmt"
	"github.com/Bowery/prompt"
	"github.com/mkideal/cli"
	_ "github.com/uglyer/ha-sqlite/driver"
	"strings"
)

type argT struct {
	cli.Helper
	Address string `cli:"a,address" usage:"ha-sqlite address" dft:"multi:///localhost:30051,localhost:30052,localhost:30053/:memory:"`
	Version bool   `cli:"v,version" usage:"display CLI version"`
}

const version = "0.1.0"

func main() {
	cli.SetUsageStyle(cli.ManualStyle)
	cli.Run(new(argT), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*argT)
		if argv.Version {
			ctx.String("version:\n", version)
			return nil
		}
		prefix := fmt.Sprintf("%s>", argv.Address)
		term, err := prompt.NewTerminal()
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}
		term.Close()
		client, err := newHaClient(argv.Address)
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}
	FOR_READ:
		for {
			term.Reopen()
			line, err := term.Basic(prefix, false)
			term.Close()
			if err != nil {
				return err
			}

			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			var (
				index = strings.Index(line, " ")
				cmd   = line
			)
			if index >= 0 {
				cmd = line[:index]
			}
			cmd = strings.ToUpper(cmd)
			switch cmd {
			case ".QUIT", "QUIT", "EXIT":
				break FOR_READ
			case ".TABLES":
				client.query(ctx, `SELECT * FROM sqlite_master WHERE type="table"`)
			case ".INDEXES":
				client.query(ctx, `SELECT * FROM sqlite_master WHERE type="index"`)
			case "SELECT", "PRAGMA":
				client.query(ctx, line)
			default:
				client.exec(ctx, line)
			}
		}
		ctx.String("bye~\n")
		return nil
	})
}
