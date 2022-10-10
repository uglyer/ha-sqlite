package walfs

/* WAL magic value. Either this value, or the same value with the least
 * significant bit also set (FORMAT__WAL_MAGIC | 0x00000001) is stored in 32-bit
 * big-endian format in the first 4 bytes of a WAL file.
 *
 * If the LSB is set, then the checksums for each frame within the WAL file are
 * calculated by treating all data as an array of 32-bit big-endian
 * words. Otherwise, they are calculated by interpreting all data as 32-bit
 * little-endian words. */
const VFS__WAL_MAGIC uint32 = 0x377f0682

/* WAL format version (same for WAL index). */
const VFS__WAL_VERSION uint32 = 3007000

const VFS__BIGENDIAN = 1

/* Minumum and maximum page size. */
const FORMAT__PAGE_SIZE_MIN = 512
const FORMAT__PAGE_SIZE_MAX = 65536

/* Database header size. */
const FORMAT__DB_HDR_SIZE = 100

/* Write ahead log header size. */
const FORMAT__WAL_HDR_SIZE = 32

/* Write ahead log frame header size. */
const FORMAT__WAL_FRAME_HDR_SIZE = 24

/* Number of reader marks in the wal index header. */
const FORMAT__WAL_NREADER = 5

/* Given the page size, calculate the size of a full WAL frame (frame header
 * plus page data). */
func formatWalCalcFrameSize(PAGE_SIZE int) int {
	return (FORMAT__WAL_FRAME_HDR_SIZE + PAGE_SIZE)
}

/* Given the page size and the WAL file size, calculate the number of frames it
 * has. */
func formatWalCalcFramesNumber(PAGE_SIZE int, SIZE int) int {
	return ((SIZE - FORMAT__WAL_HDR_SIZE) / formatWalCalcFrameSize(PAGE_SIZE))
}

/* Given the page size, calculate the WAL page number of the frame starting at
 * the given offset. */
func formatWalCalcFrameIndex(PAGE_SIZE int, OFFSET int) int {
	return (formatWalCalcFramesNumber(PAGE_SIZE, OFFSET) + 1)
}
