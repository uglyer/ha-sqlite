package walfs

import (
	"encoding/binary"
	"fmt"
	"github.com/uglyer/go-sqlite3"
	"github.com/uglyer/ha-sqlite/proto"
	gProto "google.golang.org/protobuf/proto"
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
	// frames 帧数据, 下标从 1 开始
	frames map[int]*VfsFrame
	// tx 用于存放未提交到 raft 的数据
	tx map[int]*VfsFrame
	// tx 最后一个写入的 id
	txLastIndex int
	flags       int
	fs          *WalFS
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
		tx:             map[int]*VfsFrame{},
		txLastIndex:    0,
		flags:          flags,
	}
	f.walMap[name] = newFile
	return newFile, nil
}

func (f *WalFS) GetFileBuffer(name string) (buf *VfsWal, hasFile bool) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	buf, hasFile = f.walMap[name]
	return
}

// VfsPoll 在 checkWal 中执行 vfsPoll, 拷贝tx中的所有数据提交到 raft 并 转移至 frame 中
func (f *WalFS) VfsPoll(name string) ([]byte, error, bool) {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	wal, hasFile := f.walMap[name]
	if !hasFile {
		return nil, nil, false
	}
	return wal.walTxPoll()
}

// VfsApplyLog 应用来自 raft 中的日志
func (f *WalFS) VfsApplyLog(name string, buffer []byte) error {
	f.mtx.Lock()
	wal, hasFile := f.walMap[name]
	if !hasFile {
		// 创建一个文件用于应用日志
		f.mtx.Unlock()
		wal, err := f.OpenFile(name, os.O_CREATE, 0660)
		if err != nil {
			return fmt.Errorf("VfsApplyLog error:%v", err)
		}
		return wal.walApplyLog(buffer)
	}
	defer f.mtx.Unlock()
	return wal.walApplyLog(buffer)
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

func (wal *VfsWal) GetPageSize() uint32 {
	return vfsParsePageSize(binary.BigEndian.Uint32(wal.header[8:12]))
}

func (wal *VfsWal) getWalFrameInstanceInLock(index int, pageSize int) *VfsFrame {
	if instance, ok := wal.tx[index]; ok {
		return instance
	}
	if instance, ok := wal.frames[index]; ok {
		return instance
	}
	frame := &VfsFrame{
		hasWriteHeader: false,
		hasWritePage:   false,
		header:         [VFS__FRAME_HEADER_SIZE]byte{},
		page:           make([]byte, pageSize),
		pageSize:       pageSize,
	}
	wal.tx[index] = frame
	wal.txLastIndex = index
	return frame
}

func (wal *VfsWal) lookUpWalFrameInstanceInLock(index int) (*VfsFrame, bool) {
	if instance, ok := wal.tx[index]; ok {
		return instance, ok
	}
	if instance, ok := wal.frames[index]; ok {
		return instance, ok
	}
	return nil, false
}

func (wal *VfsWal) Truncate(size int64) error {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	wal.hasWriteHeader = false
	wal.frames = map[int]*VfsFrame{}
	wal.tx = map[int]*VfsFrame{}
	wal.txLastIndex = 0
	return nil
}

func (wal *VfsWal) Sync(flag sqlite3.SyncType) error {
	return nil
}

func (wal *VfsWal) ReadAt(p []byte, offset int64) (int, error) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	amount := len(p)
	/* WAL header. */
	if offset == 0 {
		if wal.hasWriteHeader {
			return 0, fmt.Errorf("wal file:%s read header error: header is null", wal.name)
		}
		if amount != VFS__WAL_HEADER_SIZE {
			return 0, fmt.Errorf("wal file:%s read header error: size!=VFS__WAL_HEADER_SIZE", wal.name)
		}
		for i := 0; i < amount; i++ {
			p[i] = wal.header[i]
		}
		return amount, nil
	}
	if len(wal.frames) == 0 && len(wal.tx) == 0 {
		return 0, fmt.Errorf("wal file:%s read page error: frame is zero", wal.name)
	}
	pageSize := wal.GetPageSize()
	if pageSize <= 0 {
		return 0, fmt.Errorf("wal file:%s read page size error#1:%d", wal.name, pageSize)
	}
	var index int
	/* For any other frame, we expect either a header read,
	 * a checksum read, a page read or a full frame read. */
	if amount == FORMAT__WAL_FRAME_HDR_SIZE {
		if ((int(offset) - VFS__WAL_HEADER_SIZE) %
			(int(pageSize) + FORMAT__WAL_FRAME_HDR_SIZE)) != 0 {
			return 0, fmt.Errorf("wal file:%s read page size error#2:%d", wal.name, pageSize)
		}
		index = formatWalCalcFrameIndex(int(pageSize), int(offset))
	} else if amount == 8 {
		// sizeof(uint32_t) * 2
		if offset == FORMAT__WAL_FRAME_HDR_SIZE {
			/* Read the checksum from the WAL
			 * header. */
			for i := 0; i < amount; i++ {
				p[i] = wal.header[i+int(offset)]
			}
			return amount, nil
		}
		if ((int(offset) - 16 - VFS__WAL_HEADER_SIZE) %
			(int(pageSize) + FORMAT__WAL_FRAME_HDR_SIZE)) != 0 {
			return 0, fmt.Errorf("wal file:%s read page size error#3:%d", wal.name, pageSize)
		}
		index = (int(offset)-16-VFS__WAL_HEADER_SIZE)/
			(int(pageSize)+FORMAT__WAL_FRAME_HDR_SIZE) + 1
	} else if amount == int(pageSize) {
		if ((int(offset) - VFS__WAL_HEADER_SIZE -
			FORMAT__WAL_FRAME_HDR_SIZE) %
			(int(pageSize) + FORMAT__WAL_FRAME_HDR_SIZE)) != 0 {
			return 0, fmt.Errorf("wal file:%s read page size error#4:%d", wal.name, pageSize)
		}
		index = formatWalCalcFrameIndex(int(pageSize), int(offset))
	} else {
		if amount != (FORMAT__WAL_FRAME_HDR_SIZE + int(pageSize)) {
			return 0, fmt.Errorf("wal file:%s read page size error#5:%d", wal.name, pageSize)
		}
		index = formatWalCalcFrameIndex(int(pageSize), int(offset))
	}
	if index == 0 {
		// This is an attempt to read a page that was
		// never written.
		//memset(buf, 0, (size_t)amount);
		return 0, fmt.Errorf("wal file:%s read page error:this is an attempt to read a page that was never written", wal.name)
	}
	frame, ok := wal.lookUpWalFrameInstanceInLock(index)

	if !ok {
		return 0, fmt.Errorf("wal file:%s read page error: frame is null:%d", wal.name, index)
	}

	if amount == FORMAT__WAL_FRAME_HDR_SIZE {
		for i := 0; i < amount; i++ {
			p[i] = frame.header[i]
		}
	} else if amount == 8 {
		// sizeof(uint32_t)*2
		for i := 0; i < amount; i++ {
			p[i] = frame.header[i+16]
		}
	} else if amount == int(pageSize) {
		for i := 0; i < amount; i++ {
			p[i] = frame.page[i]
		}
	} else {
		for i := 0; i < FORMAT__WAL_FRAME_HDR_SIZE; i++ {
			p[i] = frame.header[i]
		}
		for i := 0; i < frame.pageSize; i++ {
			p[i+FORMAT__WAL_FRAME_HDR_SIZE] = frame.page[i]
		}
	}
	return amount, nil
}

func (wal *VfsWal) WriteAt(p []byte, offset int64) (int, error) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	amount := len(p)
	/* WAL header. */
	if offset == 0 {
		if amount != VFS__WAL_HEADER_SIZE {
			return 0, fmt.Errorf("wal file:%s write header error: size!=VFS__WAL_HEADER_SIZE", wal.name)
		}
		for i := 0; i < amount; i++ {
			wal.header[i] = p[i]
		}
		wal.hasWriteHeader = true
		return amount, nil
	}
	pageSize := wal.GetPageSize()
	if pageSize <= 0 {
		return 0, fmt.Errorf("wal file:%s write page size error#1:%d", wal.name, pageSize)
	}
	/* This is a WAL frame write. We expect either a frame
	 * header or page write. */
	if amount == FORMAT__WAL_FRAME_HDR_SIZE {
		/* Frame header write. */
		if ((int(offset) - VFS__WAL_HEADER_SIZE) %
			(int(pageSize) + FORMAT__WAL_FRAME_HDR_SIZE)) != 0 {
			return 0, fmt.Errorf("wal file:%s page size error#2:%d", wal.name, pageSize)
		}

		index := formatWalCalcFrameIndex(int(pageSize), int(offset))

		frame := wal.getWalFrameInstanceInLock(index, int(pageSize))
		err := frame.writeHeader(p)
		if err != nil {
			return 0, err
		}
	} else {
		/* Frame page write. */
		if amount != int(pageSize) {
			return 0, fmt.Errorf("wal file:%s page size error#3:%d", wal.name, pageSize)
		}
		if ((int(offset) - VFS__WAL_HEADER_SIZE -
			FORMAT__WAL_FRAME_HDR_SIZE) %
			(int(pageSize) + FORMAT__WAL_FRAME_HDR_SIZE)) != 0 {
			return 0, fmt.Errorf("wal file:%s page size error#4:%d", wal.name, pageSize)
		}

		index := formatWalCalcFrameIndex(int(pageSize), int(offset))
		frame, hasFrame := wal.lookUpWalFrameInstanceInLock(index)
		if !hasFrame {
			return 0, fmt.Errorf("wal file:%s get frame error:%d", wal.name, index)
		}
		err := frame.writePage(p)
		if err != nil {
			return 0, err
		}
	}
	return amount, nil
}

func (wal *VfsWal) Close() error {
	if wal.flags&SQLITE_OPEN_DELETEONCLOSE != 0 {
		wal.fs.DeleteFile(wal.name)
	}
	return nil
}

func (wal *VfsWal) Lock(elock sqlite3.LockType) error {
	return nil
}

func (wal *VfsWal) Unlock(elock sqlite3.LockType) error {
	return nil
}

// CheckReservedLock We always report that a lock is held. This routine should be used only in
// journal mode, so it doesn't matter.
func (wal *VfsWal) CheckReservedLock() (bool, error) {
	return true, nil
}

func (wal *VfsWal) FileSize() (int64, error) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	if !wal.hasWriteHeader {
		return 0, nil
	}
	size := int64(VFS__WAL_HEADER_SIZE)
	for _, v := range wal.frames {
		size += v.FileSize()
	}
	for _, v := range wal.tx {
		size += v.FileSize()
	}
	return size, nil
}

func (wal *VfsWal) SectorSize() int64 {
	return 0
}

func (wal *VfsWal) DeviceCharacteristics() sqlite3.DeviceCharacteristic {
	return 0
}

// walPoll 未提交日志遍历, 返回序列化的指令 (WalCommand, error info, 是否需要提交日志(当值为true但 error != nil 时表示出现重大错误 需要中断服务))
func (wal *VfsWal) walTxPoll() ([]byte, error, bool) {
	wal.mtx.Lock()
	defer wal.mtx.Unlock()
	txCount := len(wal.tx)
	if txCount == 0 {
		// 无新增
		return nil, nil, false
	}
	/* Check if the last frame in the transaction has the commit marker. */
	lastFrame, ok := wal.tx[wal.txLastIndex]
	if !ok || !lastFrame.hasWriteHeader || !lastFrame.hasWritePage {
		return nil, nil, false
	}
	if lastFrame.Commit() == 0 {
		return nil, nil, false
	}
	frames := make([]*proto.WalFrame, txCount)
	index := 0
	for k, v := range wal.tx {
		if !v.hasWriteHeader || !v.hasWritePage {
			return nil, fmt.Errorf("walTxPoll Marshal tx hasWriteHeader :%v,hasWritePage:%v", v.hasWriteHeader, v.hasWritePage), false
		}
		frames[index] = &proto.WalFrame{
			Data:       v.page,
			PageNumber: v.PageNumber(),
		}
		wal.frames[k] = v
		delete(wal.tx, k)
		index++
	}
	cmd := &proto.WalCommand{
		Frames: frames,
	}
	b, err := gProto.Marshal(cmd)
	if err != nil {
		return nil, fmt.Errorf("walTxPoll Marshal error :%v", err), true
	}
	return b, nil, true
}

func (wal *VfsWal) walApplyLog(buffer []byte) error {
	var cmd proto.WalCommand
	err := gProto.Unmarshal(buffer, &cmd)
	if err != nil {
		return fmt.Errorf("walApplyLog Unmarshal error :%v", err)
	}
	if len(cmd.Frames) == 0 {
		return nil
	}
	/* If there's no page size set in the WAL header, it must mean that WAL
	 * file was never written. In that case we need to initialize the WAL
	 * header. */
	if wal.hasWriteHeader == false {
		pageSize := len(cmd.Frames[0].Data)
		err = wal.vfsWalInitHeader(pageSize)
		if err != nil {
			return fmt.Errorf("walApplyLog vfsWalStartHeader error :%v", err)
		}
	}
	//for _, frame := range cmd.Frames {
	//
	//}
	return fmt.Errorf("todo impl VfsApplyLog")
}

func (wal *VfsWal) vfsWalInitHeader(pageSize int) error {
	if pageSize <= 0 {
		return fmt.Errorf("page size is :%d", pageSize)
	}
	// uint32_t checksum[2] = {0, 0};
	checksum := []uint32{0, 0}
	///* SQLite calculates checksums for the WAL header and frames either
	// * using little endian or big endian byte order when adding up 32-bit
	// * words. The byte order that should be used is recorded in the WAL file
	// * header by setting the least significant bit of the magic value stored
	// * in the first 32 bits. This allows portability of the WAL file across
	// * hosts with different native byte order.
	// *
	// * When creating a brand new WAL file, SQLite will set the byte order
	// * bit to match the host's native byte order, so checksums are a bit
	// * more efficient.
	// *
	// * In Dqlite the WAL file image is always generated at run time on the
	// * host, so we can always use the native byte order. */
	//vfsPut32(VFS__WAL_MAGIC | VFS__BIGENDIAN, &w->hdr[0]);
	wal.putHeaderUint32(VFS__WAL_MAGIC|VFS__BIGENDIAN, 0)
	//vfsPut32(VFS__WAL_VERSION, &w->hdr[4]);
	wal.putHeaderUint32(VFS__WAL_VERSION, 4)
	//vfsPut32(page_size, &w->hdr[8]);
	wal.putHeaderUint32(uint32(pageSize), 8)
	//vfsPut32(0, &w->hdr[12]);
	wal.putHeaderUint32(0, 12)
	//sqlite3_randomness(8, &w->hdr[16]);
	randCount := 8
	b := make([]byte, randCount)
	sqlite3.Sqlite3Randomness(randCount, b, 0)
	for i := 0; i < randCount; i++ {
		wal.header[16+i] = b[i]
	}
	//vfsChecksum(w->hdr, 24, checksum, checksum);
	err := VfsChecksum(wal.header[:], 24, checksum, checksum)
	if err != nil {
		return fmt.Errorf("vfsWalInitHeader VfsChecksum error:%d", pageSize)
	}
	//vfsPut32(checksum[0], w->hdr + 24);
	wal.putHeaderUint32(checksum[0], 24)
	//vfsPut32(checksum[1], w->hdr + 28);
	wal.putHeaderUint32(checksum[1], 28)
	return nil
}

func (wal *VfsWal) putHeaderUint32(v uint32, offset int) {
	bigEndPutUint32(wal.header[:], v, offset)
}

// VfsChecksum 生成校验位
// translate from dqlite
// Generate or extend an 8 byte checksum based on the data in array data[] and
// the initial values of in[0] and in[1] (or initial values of 0 and 0 if
// in==NULL).
//
// The checksum is written back into out[] before returning.
//
// n must be a positive multiple of 8.
func VfsChecksum(b []byte, n uint32, in []uint32, out []uint32) error {
	if len(b)%4 != 0 {
		return fmt.Errorf("VfsChecksum /assert((((uintptr_t)data)  sizeof(uint32_t)) == 0), but got:%d", len(b)%4)
	}
	cur := 0
	end := int(n / 4)

	var s1, s2 uint32
	if in != nil {
		s1 = in[0]
		s2 = in[1]
	} else {
		s1 = 0
		s2 = 0
	}

	if n <= 8 {
		return fmt.Errorf("VfsChecksum assert(n >= 8);, but got:%d", n)
	}
	if (n & 0x00000007) != 0 {
		return fmt.Errorf("VfsChecksum assert (n & 0x00000007) == 0, but got:%d", n)
	}
	if n > 65536 {
		return fmt.Errorf("VfsChecksum assert(n <= 65536), but got:%d", n)
	}

	for true {
		s1 += bigEndGetUint32(b, cur) + s2
		cur += 4
		s2 += bigEndGetUint32(b, cur) + s1
		cur += 4
		if !(cur < end) {
			break
		}
	}
	out[0] = s1
	out[1] = s2
	return nil
}

func bigEndPutUint32(b []byte, v uint32, offset int) {
	b[offset] = byte(v >> 24)
	b[offset+1] = byte(v >> 16)
	b[offset+2] = byte(v >> 8)
	b[offset+3] = byte(v)
}

func bigEndGetUint32(b []byte, offset int) uint32 {
	return uint32(b[3+offset]) | uint32(b[2+offset])<<8 | uint32(b[1+offset])<<16 | uint32(b[0+offset])<<24
}
