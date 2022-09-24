package db

import (
	"io"
	"os"
)

type HaSqliteVFS struct{}

type HaSqliteVFSFile struct {
	*os.File
	name string
	f    *os.File
	lock int
}

func (*HaSqliteVFS) Open(name string, flags int) (interface{}, error) {
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
