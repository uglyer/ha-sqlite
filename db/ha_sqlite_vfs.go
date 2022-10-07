package db

import (
	"fmt"
	"github.com/uglyer/go-sqlite3"
	"github.com/uglyer/ha-sqlite/db/walfs"
	"log"
	"os"
	"strings"
	"sync/atomic"
)

// HaSqliteVFS TODO 单独实现 wal 内存存储实现, 经验证 wal 文件始终以32字节为头文件, 每页 页头24字节，正文4096字节, 均为新增插入
// wal Frame header
// 0 4byte uint32 page_number
// 4 4byte uint32 database_size
//       For commit records, the size of the database file in pages
//		 after the commit. For all other records, zero
// 8 4byte uint32 salt 0
// 12 4byte uint32 salt 1
// 16 4byte uint32 checksum 0
// 20 4byte uint32 checksum 1
type HaSqliteVFS struct {
	rootMemFS *walfs.WalFS
}

type HaSqliteVFSFile struct {
	*os.File
	lockCount int64
	f         *os.File
}

func NewHaSqliteVFS() *HaSqliteVFS {
	rootFS := walfs.NewWalFS()
	return &HaSqliteVFS{rootMemFS: rootFS}
}

func (vfs *HaSqliteVFS) Open(name string, flags sqlite3.OpenFlag) (sqlite3.File, sqlite3.OpenFlag, error) {
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
	if strings.HasSuffix(name, "-wal") {
		file, err := vfs.rootMemFS.OpenFile(name, fileFlags, 0600)
		if err != nil {
			log.Printf("open wal error:%s", err)
			return nil, 0, err
		}
		return file, 0, nil
	}
	var (
		f   *os.File
		err error
	)
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
	info, err := tf.f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
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
