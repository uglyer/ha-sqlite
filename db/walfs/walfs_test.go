package walfs

import (
	"github.com/stretchr/testify/assert"
	"github.com/uglyer/ha-sqlite/proto"
	gProto "google.golang.org/protobuf/proto"
	"math/rand"
	"os"
	"testing"
)

func TestVfsChecksum(t *testing.T) {
	checksum := []uint32{0, 0}
	checkCount := 32
	buffer := [32]byte{}
	for i := 0; i < checkCount; i++ {
		buffer[i] = uint8(rand.Uint32())
	}
	err := VfsChecksum(buffer[:], uint32(checkCount), checksum, checksum)
	assert.NoError(t, err)
	assert.NotEqual(t, checksum[0], 0)
	assert.NotEqual(t, checksum[1], 0)
}

func TestVfsWalApplyLog(t *testing.T) {
	walFS := NewWalFS()
	pageSize := 4096
	buffer := make([]byte, pageSize)
	for i := 0; i < pageSize; i++ {
		buffer[i] = uint8(rand.Uint32())
	}
	cmd := &proto.WalCommand{
		Frames: []*proto.WalFrame{
			{
				PageNumber: 1,
				Data:       buffer,
			},
		},
	}

	logBuffer, err := gProto.Marshal(cmd)
	assert.NoError(t, err)

	t.Run("without wal file test", func(t *testing.T) {
		err := walFS.VfsApplyLog("test", logBuffer)
		assert.NoError(t, err)

		walFile, err := walFS.OpenFile("test", os.O_RDWR, nil)
		assert.NoError(t, err)

		assert.Equal(t, true, walFile.hasWriteHeader)

		assert.Equal(t, VFS__WAL_HEADER_SIZE, len(walFile.header))
		for i := 0; i < len(walFile.header); i++ {
			assert.NotEqual(t, 0, walFile.header[i])
		}

		assert.Equal(t, len(cmd.Frames), len(walFile.frames))

		for index, frame := range cmd.Frames {
			walFrame := walFile.frames[index]
			assert.Equal(t, true, walFrame.hasWriteHeader)
			assert.Equal(t, true, walFrame.hasWritePage)
			assert.Equal(t, pageSize, walFrame.pageSize)

			for i := 0; i < pageSize; i++ {
				assert.Equal(t, frame.Data[i], walFile.frames[index].page[i])
			}
		}
	})

}
