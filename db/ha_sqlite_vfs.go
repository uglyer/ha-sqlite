package db

import "C"
import (
	"github.com/uglyer/ha-sqlite/db/memfs"
	"io"
	"os"
)

type HaSqliteVFS struct {
	rootMemFS *memfs.FS
}

type HaSqliteVFSFile struct {
	*os.File
	name string
	f    *os.File
	lock int
}

func NewHaSqliteVFS() *HaSqliteVFS {
	rootFS := memfs.NewFS()
	return &HaSqliteVFS{rootMemFS: rootFS}
}

func (v *HaSqliteVFS) Open(name string, flags int) (interface{}, error) {
	//log.Printf("vfs.open:%s", name)
	// TODO vfs 实现 sqlite3_io_methods 中 xTruncate,xShmMap,xShmLock,xShmBarrier,xShmUnmap,xFetch,xUnfetch 以支持 wal 模式
	//if strings.HasSuffix(name, "-wal") {
	//	file, err := v.rootMemFS.OpenFile(name, flags, 0600)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return file, nil
	//}
	file, err := os.OpenFile(name, flags, 0600)
	if err != nil {
		return nil, err
	}
	return &HaSqliteVFSFile{file, name, file, 0}, nil
}

func (f *HaSqliteVFSFile) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.f.ReadAt(p, off)
	if err == io.EOF {
		err = nil
	}
	return
}

func (f *HaSqliteVFSFile) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = f.f.WriteAt(p, off)
	return
}

//func (f *HaSqliteVFSFile) DeviceCharacteristics() int {
//	return 0x00004000
//}
//
//// Access sqlite3 内部会在 pagerOpenWalIfPresent 中被调用
//func (f *HaSqliteVFSFile) Access(path string, flags int) (int, error) {
//	log.Printf("vfs.access:%s,%d", path, flags)
//	return sqlite3.SQLITE_OK, nil
//}
