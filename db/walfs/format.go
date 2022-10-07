package walfs

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
