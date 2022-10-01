package db

import (
	"fmt"
	"github.com/uglyer/go-sqlite3"
	"github.com/uglyer/ha-sqlite/db/memfs"
	"log"
	"os"
	"sync/atomic"
)

type HaSqliteVFS struct {
	rootMemFS *memfs.FS
}

type HaSqliteVFSFile struct {
	*os.File
	lockCount int64
	f         *os.File
}

func NewHaSqliteVFS() *HaSqliteVFS {
	rootFS := memfs.NewFS()
	return &HaSqliteVFS{rootMemFS: rootFS}
}

func (vfs *HaSqliteVFS) Open(name string, flags sqlite3.OpenFlag) (sqlite3.File, sqlite3.OpenFlag, error) {
	//log.Printf("vfs.open:%s", name)
	// TODO vfs 实现 sqlite3_io_methods 中 xTruncate,xShmMap,xShmLock,xShmBarrier,xShmUnmap,xFetch,xUnfetch 以支持 wal 模式
	//if strings.HasSuffix(name, "-wal") {
	//	file, err := v.rootMemFS.OpenFile(name, flags, 0600)
	//	if err != nil {
	//		return nil, err
	//	}
	//	return file, nil
	//}
	var (
		f   *os.File
		err error
	)
	log.Printf("vfs.open:%s", name)
	if name == "" {
		return nil, flags, fmt.Errorf("")
	}
	var fileFlags int
	if flags&sqlite3.OpenExclusive != 0 {
		fileFlags |= os.O_EXCL
	}
	if flags&sqlite3.OpenCreate != 0 {
		fileFlags |= os.O_CREATE
	}
	if flags&sqlite3.OpenReadOnly != 0 {
		fileFlags |= os.O_RDONLY
	}
	if flags&sqlite3.OpenReadWrite != 0 {
		fileFlags |= os.O_RDWR
	}
	f, err = os.OpenFile(name, fileFlags, 0600)
	if err != nil {
		return nil, 0, sqlite3.CantOpenError
	}

	tf := &HaSqliteVFSFile{f: f}
	return tf, flags, nil
}

func (vfs *HaSqliteVFS) Delete(name string, dirSync bool) error {
	log.Printf("vfs.delete:%s", name)
	return os.Remove(name)
}

func (vfs *HaSqliteVFS) Access(name string, flag sqlite3.AccessFlag) (bool, error) {
	exists := true
	_, err := os.Stat(name)
	if err != nil && os.IsNotExist(err) {
		exists = false
	} else if err != nil {
		return false, err
	}

	if flag == sqlite3.AccessExists {
		return exists, nil
	}
	return true, nil
}

func (vfs *HaSqliteVFS) FullPathname(name string) string {
	return name
}

func (tf *HaSqliteVFSFile) Close() error {
	return tf.f.Close()
}

func (tf *HaSqliteVFSFile) ReadAt(p []byte, off int64) (n int, err error) {
	return tf.f.ReadAt(p, off)
}

func (tf *HaSqliteVFSFile) WriteAt(b []byte, off int64) (n int, err error) {
	return tf.f.WriteAt(b, off)
}

func (tf *HaSqliteVFSFile) Truncate(size int64) error {
	return tf.f.Truncate(size)
}

func (tf *HaSqliteVFSFile) Sync(flag sqlite3.SyncType) error {
	return tf.f.Sync()
}

func (tf *HaSqliteVFSFile) FileSize() (int64, error) {
	cur, _ := tf.f.Seek(0, os.SEEK_CUR)
	end, err := tf.f.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}

	tf.f.Seek(cur, os.SEEK_SET)
	return end, nil
}

func (tf *HaSqliteVFSFile) Lock(elock sqlite3.LockType) error {
	if elock == sqlite3.LockNone {
		return nil
	}
	atomic.AddInt64(&tf.lockCount, 1)
	return nil
}

func (tf *HaSqliteVFSFile) Unlock(elock sqlite3.LockType) error {
	if elock == sqlite3.LockNone {
		return nil
	}
	atomic.AddInt64(&tf.lockCount, -1)
	return nil
}

func (tf *HaSqliteVFSFile) CheckReservedLock() (bool, error) {
	count := atomic.LoadInt64(&tf.lockCount)
	return count > 0, nil
}

func (tf *HaSqliteVFSFile) SectorSize() int64 {
	return 0
}

func (tf *HaSqliteVFSFile) DeviceCharacteristics() sqlite3.DeviceCharacteristic {
	return 0
}
