package walfs

import (
	"fmt"
	"github.com/uglyer/go-sqlite3"
	"os"
	"sync"
)

/* Write ahead log header size. */
const VFS__WAL_HEADER_SIZE = 32

/* Write ahead log frame header size. */
const VFS__FRAME_HEADER_SIZE = 24

/* Size of the first part of the WAL index header. */
const VFS__WAL_INDEX_HEADER_SIZE = 48

/* Size of a single memory-mapped WAL index region. */
const VFS__WAL_INDEX_REGION_SIZE = 32768

const SQLITE_OPEN_DELETEONCLOSE int = 0x00000008 /* VFS only */

// WalFS is an in-memory filesystem that implements wal io
type WalFS struct {
	walMap map[string]*VfsWal
	mtx    sync.Mutex
}

type VfsWal struct {
	name        string
	mtx         sync.Mutex
	writeHeader bool
	header      [VFS__WAL_HEADER_SIZE]byte
	frames      []VfsFrame
	flags       int
	fs          *WalFS
}

type VfsFrame struct {
	writeHeader bool
	header      [VFS__FRAME_HEADER_SIZE]byte
	page        []byte
}

// NewWalFS creates a new in-memory wal FileSystem.
func NewWalFS() *WalFS {
	return &WalFS{
		walMap: make(map[string]*VfsWal),
	}
}

// OpenFile is the generalized open call; most users will use Open
// or Create instead. It opens the named file with specified flag
// (O_RDONLY etc.). If the file does not exist, and the O_CREATE flag
// is passed, it is created with mode perm (before umask). If successful,
// methods on the returned MemFile can be used for I/O.
// If there is an error, it will be of type *PathError.
func (f *WalFS) OpenFile(name string, flags int, perm os.FileMode) (*VfsWal, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	b, hasFile := f.walMap[name]
	if flags&os.O_EXCL != 0 && hasFile {
		return nil, fmt.Errorf("The file already exists:%s", name)
	}
	if hasFile {
		return b, nil
	}
	newFile := &VfsWal{
		name:        name,
		writeHeader: false,
		frames:      []VfsFrame{},
		flags:       flags,
	}
	f.walMap[name] = newFile
	return newFile, nil
}

func (f *WalFS) DeleteFile(name string) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	b, hasFile := f.walMap[name]
	if !hasFile {
		return
	}
	b.fs = nil
	delete(f.walMap, name)
}

func (f *VfsWal) Close() error {
	if f.flags&SQLITE_OPEN_DELETEONCLOSE != 0 {
		f.fs.DeleteFile(f.name)
	}
	return nil
}

func (f *VfsWal) Lock(elock sqlite3.LockType) error {
	return nil
}

func (f *VfsWal) Unlock(elock sqlite3.LockType) error {
	return nil
}

// CheckReservedLock We always report that a lock is held. This routine should be used only in
// journal mode, so it doesn't matter.
func (f *VfsWal) CheckReservedLock() (bool, error) {
	return true, nil
}

func (f *VfsWal) FileSize() (int64, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	if !f.writeHeader {
		return 0, nil
	}
	size := int64(VFS__WAL_HEADER_SIZE)
	count := len(f.frames)
	for i := 0; i < count; i++ {
		size += f.frames[i].FileSize()
	}
	return size, nil
}

func (f *VfsFrame) FileSize() int64 {
	if !f.writeHeader {
		return 0
	}
	return int64(len(f.page))
}

func (f *VfsWal) SectorSize() int64 {
	return 0
}

func (f *VfsWal) DeviceCharacteristics() sqlite3.DeviceCharacteristic {
	return 0
}
