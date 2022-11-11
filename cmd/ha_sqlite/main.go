package main

import (
	"fmt"
	"github.com/Bowery/prompt"
	"github.com/mkideal/cli"
	_ "github.com/uglyer/ha-sqlite/driver"
	"github.com/uglyer/ha-sqlite/proto"
	"strings"
	"sync"
	"time"
)

type argT struct {
	cli.Helper
	Address string `cli:"a,address" usage:"ha-sqlite address" dft:"multi:///localhost:30051,localhost:30052,localhost:30053/"`
	Version bool   `cli:"v,version" usage:"display CLI version"`
	Debug   bool   `cli:"d,debug" usage:"debug"`
}

const version = "0.1.0"

func main() {
	proto.SetIsPrintCoastTime(true)
	cli.SetUsageStyle(cli.ManualStyle)
	cli.Run(new(argT), func(ctx *cli.Context) error {
		argv := ctx.Argv().(*argT)
		if argv.Version {
			ctx.String("version:\n", version)
			return nil
		}
		dbName := "default"
		dbNameStartIndex := strings.LastIndex(argv.Address, "/")
		if !strings.HasSuffix(argv.Address, "/") {
			dbName = argv.Address[dbNameStartIndex+1:]
		}
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
		client.setDbName(ctx, dbName)
		if argv.Debug {
			// todo 连续执行插入语句响应过慢
			startT := time.Now()
			var wg sync.WaitGroup
			for i := 0; i < 1000; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					client.exec(ctx, "INSERT INTO foo(name) VALUES(\"xxx\")")
					//rows, err := client.db.Query("select count(*) from foo")
					//if err != nil {
					//	return
					//}
					//rows.Close()
				}()
			}
			wg.Wait()
			tc := time.Since(startT) //计算耗时
			fmt.Printf("time cost = %v\n", tc)
		}
	FOR_READ:
		for {
			term.Reopen()
			line, err := term.Basic(client.prefix(), false)
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
				client.queryWithDBName(ctx, `SELECT * FROM sqlite_master WHERE type="table"`)
			case ".INDEXES":
				client.queryWithDBName(ctx, `SELECT * FROM sqlite_master WHERE type="index"`)
			case "SELECT", "PRAGMA":
				client.queryWithDBName(ctx, line)
			default:
				client.execWithDBName(ctx, line)
			}
		}
		ctx.String("bye~\n")
		return nil
	})
}
