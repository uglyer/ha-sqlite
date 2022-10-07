package walfs

import "fmt"

type VfsFrame struct {
	hasWriteHeader bool
	header         [VFS__FRAME_HEADER_SIZE]byte
	page           []byte
}

func (f *VfsFrame) writeHeader(p []byte) error {
	if len(p) != VFS__FRAME_HEADER_SIZE {
		return fmt.Errorf("header size error:%d", p)
	}
	f.hasWriteHeader = true
	for i := 0; i < VFS__FRAME_HEADER_SIZE; i++ {
		f.header[i] = p[i]
	}
	return nil
}
