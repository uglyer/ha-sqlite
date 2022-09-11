package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/Bowery/prompt"
	"github.com/mkideal/cli"
	_ "github.com/uglyer/ha-sqlite/driver"
	"runtime"
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
		ctx.String("open:%s\n", argv.Address)
		db, err := sql.Open("ha-sqlite", argv.Address)
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}
		db.SetMaxIdleConns(runtime.NumCPU() * 2)
		db.SetMaxOpenConns(runtime.NumCPU() * 2)
		ctx.String("ping~\n")
		err = db.PingContext(context.Background())
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}
		ctx.String("connected\n")
		//timer := false
		//consistency := "weak"
		prefix := fmt.Sprintf("%s>", argv.Address)
		term, err := prompt.NewTerminal()
		if err != nil {
			ctx.String("%s %v\n", ctx.Color().Red("ERR!"), err)
			return nil
		}
		term.Close()
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
			case "SELECT", "PRAGMA":
				ctx.String("query:%s\n", line)
			default:
				ctx.String("exec:%s\n", line)
			}
		}
		ctx.String("bye~\n")
		return nil
	})
}
