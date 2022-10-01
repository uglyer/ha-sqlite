package memfs

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/uglyer/go-sqlite3"
	"io"
	"io/fs"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// FS is an in-memory filesystem that implements
// io/fs.FS
type FS struct {
	bufferMap map[string]*MemBuffer
	mtx       sync.Mutex
}

// NewFS creates a new in-memory FileSystem.
func NewFS() *FS {
	return &FS{
		bufferMap: make(map[string]*MemBuffer),
	}
}

// OpenFile is the generalized open call; most users will use Open
// or Create instead. It opens the named file with specified flag
// (O_RDONLY etc.). If the file does not exist, and the O_CREATE flag
// is passed, it is created with mode perm (before umask). If successful,
// methods on the returned MemFile can be used for I/O.
// If there is an error, it will be of type *PathError.
func (f *FS) OpenFile(name string, flags int, perm os.FileMode) (*MemFile, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	b, hasFile := f.bufferMap[name]
	if flags&os.O_EXCL != 0 && hasFile {
		return nil, fmt.Errorf("The file already exists:%s", name)
	}
	if flags&os.O_CREATE != 0 && hasFile && b.CheckReservedLock() {
		return nil, fmt.Errorf("The file is locked:%s", name)
	}
	if hasFile {
		b.Lock()
		return &MemFile{
			name:       name,
			perm:       perm,
			content:    b,
			appendMode: flags&os.O_APPEND != 0,
		}, nil
	}
	buffer := &MemBuffer{content: []byte{}}
	buffer.Lock()
	newFile := &MemFile{
		name:       name,
		perm:       perm,
		content:    buffer,
		appendMode: flags&os.O_APPEND != 0,
	}
	f.bufferMap[name] = buffer
	return newFile, nil
}

type MemFile struct {
	name       string
	perm       os.FileMode
	content    *MemBuffer
	modTime    time.Time
	lockCount  int64
	appendMode bool
	closed     bool
}

func (f *MemFile) Stat() (fs.FileInfo, error) {
	if f.closed {
		return nil, fs.ErrClosed
	}
	fi := fileInfo{
		name:    f.name,
		size:    f.content.Len(),
		modTime: f.modTime,
		mode:    f.perm,
	}
	return &fi, nil
}

func (f *MemFile) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.content.ReadAt(p, off)
	if err == io.EOF {
		err = nil
	}
	return
}

func (f *MemFile) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = f.content.WriteAt(p, off)
	return
}

func (f *MemFile) Close() error {
	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true
	f.content.Unlock()
	return nil
}

func (f *MemFile) Truncate(size int64) error {
	//return f.f.Truncate(size)
}

func (f *MemFile) Sync(flag sqlite3.SyncType) error {
	return nil
}

func (f *MemFile) FileSize() (int64, error) {
	return f.content.Len(), nil
}

func (f *MemFile) Lock(elock sqlite3.LockType) error {
	if elock == sqlite3.LockNone {
		return nil
	}
	atomic.AddInt64(&f.lockCount, 1)
	return nil
}

func (f *MemFile) Unlock(elock sqlite3.LockType) error {
	if elock == sqlite3.LockNone {
		return nil
	}
	atomic.AddInt64(&f.lockCount, -1)
	return nil
}

func (f *MemFile) CheckReservedLock() (bool, error) {
	count := atomic.LoadInt64(&f.lockCount)
	return count > 0, nil
}

func (f *MemFile) SectorSize() int64 {
	return 0
}

func (f *MemFile) DeviceCharacteristics() sqlite3.DeviceCharacteristic {
	return 0
}

type fileInfo struct {
	name    string
	size    int64
	modTime time.Time
	mode    fs.FileMode
}

// base name of the file
func (fi *fileInfo) Name() string {
	return fi.name
}

// length in bytes for regular files; system-dependent for others
func (fi *fileInfo) Size() int64 {
	return fi.size
}

// file mode bits
func (fi *fileInfo) Mode() fs.FileMode {
	return fi.mode
}

// modification time
func (fi *fileInfo) ModTime() time.Time {
	return fi.modTime
}

// abbreviation for Mode().IsDir()
func (fi *fileInfo) IsDir() bool {
	return fi.mode&fs.ModeDir > 0
}

// underlying data source (can return nil)
func (fi *fileInfo) Sys() interface{} {
	return nil
}

type dirEntry struct {
	info fs.FileInfo
}

func (de *dirEntry) Name() string {
	return de.info.Name()
}

func (de *dirEntry) IsDir() bool {
	return de.info.IsDir()
}

func (de *dirEntry) Type() fs.FileMode {
	return de.info.Mode()
}

func (de *dirEntry) Info() (fs.FileInfo, error) {
	return de.info, nil
}

type MemBuffer struct {
	mtx        sync.Mutex
	content    []byte
	contentLen int64
	lockCount  int64
}

func (b *MemBuffer) Len() int64 {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return b.contentLen
}

func (b *MemBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	if off >= b.contentLen {
		return 0, io.EOF
	}
	readLen := int64(len(p))
	realReadLen := b.contentLen - off
	if realReadLen >= readLen {
		realReadLen = readLen
	}
	for i := int64(0); i < realReadLen; i++ {
		p[i] = b.content[i+off]
	}
	return int(realReadLen), nil
}

func (b *MemBuffer) WriteAt(p []byte, off int64) (int, error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	pageLen := int64(len(b.content))
	writeLen := int64(len(p))
	freeLen := pageLen - off
	if freeLen < writeLen {
		// 需要扩容
		nextLen := pageLen + 8192*int64(math.Ceil(float64(writeLen)/float64(8192)))
		nextBuffer := make([]byte, nextLen)
		// 拷贝原字节
		for i := int64(0); i < b.contentLen; i++ {
			nextBuffer[i] = b.content[i]
		}
		b.content = nextBuffer
	}
	for i := int64(0); i < writeLen; i++ {
		b.content[i+off] = p[i]
	}
	nextLen := writeLen + off
	if nextLen > b.contentLen {
		b.contentLen = nextLen
	}
	return int(writeLen), nil
}

func (b *MemBuffer) CheckReservedLock() bool {
	count := atomic.LoadInt64(&b.lockCount)
	return count > 0
}

func (tf *MemBuffer) Lock() {
	atomic.AddInt64(&tf.lockCount, 1)
}

func (tf *MemBuffer) Unlock() {
	atomic.AddInt64(&tf.lockCount, -1)
}
