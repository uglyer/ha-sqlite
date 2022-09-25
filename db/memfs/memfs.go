package memfs

import (
	"io"
	"io/fs"
	"os"
	"sync"
	"time"
)

// FS is an in-memory filesystem that implements
// io/fs.FS
type FS struct {
	fileMap map[string]*File
	mtx     sync.Mutex
}

// NewFS creates a new in-memory FileSystem.
func NewFS() *FS {
	return &FS{
		fileMap: make(map[string]*File),
	}
}

// OpenFile is the generalized open call; most users will use Open
// or Create instead. It opens the named file with specified flag
// (O_RDONLY etc.). If the file does not exist, and the O_CREATE flag
// is passed, it is created with mode perm (before umask). If successful,
// methods on the returned File can be used for I/O.
// If there is an error, it will be of type *PathError.
func (f *FS) OpenFile(name string, flag int, perm os.FileMode) (*File, error) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	if file, ok := f.fileMap[name]; ok {
		file.appendMode = flag&os.O_APPEND != 0
		file.closed = false
		return file, nil
	}
	newFile := &File{
		name:       name,
		perm:       0666,
		content:    &MemBuffer{},
		appendMode: flag&os.O_APPEND != 0,
	}
	f.fileMap[name] = newFile
	return newFile, nil
}

type File struct {
	name       string
	perm       os.FileMode
	content    *MemBuffer
	modTime    time.Time
	appendMode bool
	closed     bool
}

func (f *File) Stat() (fs.FileInfo, error) {
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

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	n, err = f.content.ReadAt(p, off)
	if err == io.EOF {
		err = nil
	}
	return
}

func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	n, err = f.content.WriteAt(p, off)
	return
}

func (f *File) Close() error {
	if f.closed {
		return fs.ErrClosed
	}
	f.closed = true
	return nil
}

type childI interface {
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
	mtx     sync.Mutex
	content []byte
}

func (b *MemBuffer) Len() int64 {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	return int64(len(b.content))
}

func (b *MemBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	b.mtx.Lock()
	defer b.mtx.Unlock()
	bLen := b.Len()
	if off >= bLen {
		return 0, io.EOF
	}
	readLen := int64(len(p))
	realReadLen := bLen - off
	if realReadLen >= readLen {
		realReadLen = readLen
	}
	for i := int64(0); i < realReadLen; i++ {
		p[i] = b.content[i+off]
	}
	return int(realReadLen), nil
}

func (f *MemBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	//n, err = f.content.WriteAt(p, off)
	return
}
