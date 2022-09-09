package main

import (
	"context"
	"database/sql"
	"github.com/mkideal/cli"
	_ "github.com/uglyer/ha-sqlite/driver"
	"runtime"
)

type argT struct {
	cli.Helper
	Address string `cli:"a,address" usage:"ha-sqlite address" dft:"multi:///localhost:30051,localhost:30052,localhost:30053/:memory:"`
	Version bool   `cli:"v,version" usage:"display CLI version"`
}

func main() {
	cli.SetUsageStyle(cli.ManualStyle)
	cli.Run(new(argT), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*argT)
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
		return nil
	})
}
