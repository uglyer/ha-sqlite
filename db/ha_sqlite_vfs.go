package db

import (
	"github.com/uglyer/ha-sqlite/db/memfs"
	"io"
	"os"
	"strings"
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
	if strings.HasSuffix(name, "-wal") {
		file, err := v.rootMemFS.OpenFile(name, flags, 0600)
		if err != nil {
			return nil, err
		}
		return file, nil
	}
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
