package walfs

import (
	"encoding/binary"
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
	name           string
	mtx            sync.Mutex
	hasWriteHeader bool
	header         [VFS__WAL_HEADER_SIZE]byte
	frames         map[int]*VfsFrame
	flags          int
	fs             *WalFS
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
		name:           name,
		hasWriteHeader: false,
		header:         [VFS__WAL_HEADER_SIZE]byte{},
		frames:         map[int]*VfsFrame{},
		flags:          flags,
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

/* Parse the page size ("Must be a power of two between 512 and 32768
 * inclusive, or the value 1 representing a page size of 65536").
 *
 * Return 0 if the page size is out of bound. */
func vfsParsePageSize(page_size uint32) uint32 {
	if page_size == 1 {
		page_size = FORMAT__PAGE_SIZE_MAX
	} else if page_size < FORMAT__PAGE_SIZE_MIN {
		page_size = 0
	} else if page_size > (FORMAT__PAGE_SIZE_MAX / 2) {
		page_size = 0
	} else if ((page_size - 1) & page_size) != 0 {
		page_size = 0
	}
	return page_size
}

func (f *VfsWal) GetPageSize() uint32 {
	return vfsParsePageSize(binary.BigEndian.Uint32(f.header[4:8]))
}

func (f *VfsWal) getWalFrameInstanceInLock(index int, pageSize int) *VfsFrame {
	if instance, ok := f.frames[index]; ok {
		return instance
	}
	frame := &VfsFrame{
		hasWriteHeader: false,
		hasWritePage:   false,
		header:         [VFS__FRAME_HEADER_SIZE]byte{},
		page:           make([]byte, pageSize),
		pageSize:       pageSize,
	}
	f.frames[index] = frame
	return frame
}

func (f *VfsWal) WriteAt(p []byte, offset int64) (int, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	amount := len(p)
	/* WAL header. */
	if offset == 0 {
		if amount != VFS__WAL_HEADER_SIZE {
			return 0, fmt.Errorf("wal file:%s write header error: size!=VFS__WAL_HEADER_SIZE", f.name)
		}
		for i := 0; i < amount; i++ {
			f.header[i] = p[i]
		}
		f.hasWriteHeader = true
		return amount, nil
	}
	pageSize := f.GetPageSize()
	if pageSize <= 0 {
		return 0, fmt.Errorf("wal file:%s page size error#1:%d", f.name, pageSize)
	}
	/* This is a WAL frame write. We expect either a frame
	 * header or page write. */
	if amount == FORMAT__WAL_FRAME_HDR_SIZE {
		/* Frame header write. */
		if ((int(offset) - VFS__WAL_HEADER_SIZE) %
			(int(pageSize) + FORMAT__WAL_FRAME_HDR_SIZE)) != 0 {
			return 0, fmt.Errorf("wal file:%s page size error#2:%d", f.name, pageSize)
		}

		index := formatWalCalcFrameIndex(int(pageSize), int(offset))

		frame := f.getWalFrameInstanceInLock(index, int(pageSize))
		err := frame.writeHeader(p)
		if err != nil {
			return 0, err
		}
	} else {
		/* Frame page write. */
		if amount != int(pageSize) {
			return 0, fmt.Errorf("wal file:%s page size error#3:%d", f.name, pageSize)
		}
		if ((int(offset) - VFS__WAL_HEADER_SIZE -
			FORMAT__WAL_FRAME_HDR_SIZE) %
			(int(pageSize) + FORMAT__WAL_FRAME_HDR_SIZE)) != 0 {
			return 0, fmt.Errorf("wal file:%s page size error#4:%d", f.name, pageSize)
		}

		index := formatWalCalcFrameIndex(int(pageSize), int(offset))
		frame, hasFrame := f.frames[index]
		if !hasFrame {
			return 0, fmt.Errorf("wal file:%s get frame error:%d", f.name, index)
		}
		err := frame.writePage(p)
		if err != nil {
			return 0, err
		}
	}
	return amount, nil
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
	if !f.hasWriteHeader {
		return 0, nil
	}
	size := int64(VFS__WAL_HEADER_SIZE)
	count := len(f.frames)
	for i := 0; i < count; i++ {
		size += f.frames[i].FileSize()
	}
	return size, nil
}

func (f *VfsWal) SectorSize() int64 {
	return 0
}

func (f *VfsWal) DeviceCharacteristics() sqlite3.DeviceCharacteristic {
	return 0
}
