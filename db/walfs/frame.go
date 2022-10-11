package walfs

import (
	"encoding/binary"
	"fmt"
)

type VfsFrame struct {
	hasWriteHeader bool
	hasWritePage   bool
	header         [VFS__FRAME_HEADER_SIZE]byte
	page           []byte
	pageSize       int
}

func NewVfsFrame(pageSize int) *VfsFrame {
	return &VfsFrame{
		hasWriteHeader: false,
		hasWritePage:   false,
		header:         [VFS__FRAME_HEADER_SIZE]byte{},
		page:           make([]byte, pageSize),
		pageSize:       pageSize,
	}
}

func (f *VfsFrame) FrameFill(pageNumber uint32, databaseSize uint32, salt [2]uint32, checksum [2]uint32, page []byte, pageSize uint32) {

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

// Commit 调用前需要确保头数据已经写入
func (f *VfsFrame) Commit() uint32 {
	return binary.BigEndian.Uint32(f.header[4:8])
}

// PageNumber 调用前需要确保头数据已经写入
func (f *VfsFrame) PageNumber() uint32 {
	return binary.BigEndian.Uint32(f.header[0:4])
}

// DatabaseSize 调用前需要确保头数据已经写入
func (f *VfsFrame) DatabaseSize() uint32 {
	return binary.BigEndian.Uint32(f.header[4:8])
}

// getChecksum1 调用前需要确保头数据已经写入
func (f *VfsFrame) getChecksum1() uint32 {
	return binary.BigEndian.Uint32(f.header[16:20])
}

// getChecksum2 调用前需要确保头数据已经写入
func (f *VfsFrame) getChecksum2() uint32 {
	return binary.BigEndian.Uint32(f.header[20:24])
}
