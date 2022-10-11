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

func (f *VfsFrame) FrameFill(pageNumber uint32, databaseSize uint32, salt [2]uint32, checksum []uint32, page []byte, pageSize uint32) error {
	//vfsPut32(page_number, &f->header[0]);
	bigEndPutUint32(f.header[:], pageNumber, 0)
	//vfsPut32(database_size, &f->header[4]);
	bigEndPutUint32(f.header[:], pageNumber, 4)

	//vfsChecksum(f->header, 8, checksum, checksum);
	err := VfsChecksum(f.header[:], 8, checksum, checksum)
	if err != nil {
		return fmt.Errorf("FrameFill VfsChecksum error#1:%v", err)
	}
	//vfsChecksum(page, page_size, checksum, checksum);
	err = VfsChecksum(page, pageSize, checksum, checksum)
	if err != nil {
		return fmt.Errorf("FrameFill VfsChecksum error#2:%v", err)
	}

	//memcpy(&f->header[8], &salt[0], sizeof salt[0]);
	bigEndPutUint32(f.header[:], salt[0], 8)
	//memcpy(&f->header[12], &salt[1], sizeof salt[1]);
	bigEndPutUint32(f.header[:], salt[1], 12)

	//vfsPut32(checksum[0], &f->header[16]);
	bigEndPutUint32(f.header[:], checksum[0], 16)
	//vfsPut32(checksum[1], &f->header[20]);
	bigEndPutUint32(f.header[:], checksum[1], 20)

	//memcpy(f->page, page, page_size);
	size := int(pageSize)
	for i := 0; i < size; i++ {
		f.page[i] = page[i]
	}
	f.hasWriteHeader = true
	f.hasWritePage = true
	return nil
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
