package walfs

import "fmt"

type VfsFrame struct {
	hasWriteHeader bool
	hasWritePage   bool
	header         [VFS__FRAME_HEADER_SIZE]byte
	page           []byte
	pageSize       int
}

func (f *VfsFrame) writeHeader(p []byte) error {
	if len(p) != VFS__FRAME_HEADER_SIZE {
		return fmt.Errorf("header size error:%d", p)
	}
	for i := 0; i < VFS__FRAME_HEADER_SIZE; i++ {
		f.header[i] = p[i]
	}
	f.hasWriteHeader = true
	return nil
}

func (f *VfsFrame) writePage(p []byte) error {
	if len(p) != f.pageSize {
		return fmt.Errorf("page size error:%d", p)
	}
	for i := 0; i < f.pageSize; i++ {
		f.page[i] = p[i]
	}
	f.hasWritePage = true
	return nil
}

func (f *VfsFrame) FileSize() int64 {
	if !f.hasWriteHeader {
		return 0
	}
	if !f.hasWritePage {
		return VFS__FRAME_HEADER_SIZE
	}
	return int64(f.pageSize) + VFS__FRAME_HEADER_SIZE
}
