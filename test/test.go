package main

import (
	"database/sql"
	"fmt"
	"github.com/psanford/memfs"
	"io/ioutil"
	"log"
	_ "modernc.org/sqlite"
	"modernc.org/sqlite/vfs"
	"os"
	"runtime"
	"sync"
	"time"
)

func main() {
	// TODO modernc.org/sqlite vfs 支持测试
	rootFS := memfs.New()
	fn, f, err := vfs.New(rootFS)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := f.Close(); err != nil {
			log.Fatal(err)
		}
	}()

	tempFile, err := ioutil.TempFile("", "ha-sqlite-db-test")
	if err != nil {
		log.Fatalf("打开文件失败:%v", err)
	}
	//assert.NoError(t, err)
	defer os.Remove(tempFile.Name())
	defer tempFile.Close()
	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?vfs=%s", tempFile.Name(), fn))
	if err != nil {
		log.Fatalf("打开数据库失败:%v", err)
	}
	//assert.NoError(t, err)
	db.Exec("CREATE TABLE foo (id integer not null primary key, name text)")
	db.Exec("PRAGMA synchronous = OFF")
	_, err = db.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		log.Fatalf("set journal_mode = WAL error:%v", err)
	}
	count := 100000
	start := time.Now()
	ch := make(chan struct{}, runtime.NumCPU()*2)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		ch <- struct{}{}
		go func() {
			defer wg.Done()
			db.Exec("INSERT INTO foo(name) VALUES(?)", "test")
			<-ch
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("异步插入%d条记录耗时:%v,qps:%d", count, elapsed, int64(float64(count)/elapsed.Seconds()))
}
