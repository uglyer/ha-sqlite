package walfs

import (
	"encoding/binary"
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

func TestVfsChecksum2(t *testing.T) {
	headerBuffer := []byte{55, 127, 6, 130, 0, 45, 226, 24, 0, 0, 16, 0, 0, 0, 0, 0, 77, 12, 4, 129, 153, 189, 205, 69, 85, 235, 15, 96, 83, 160, 164, 158}
	checksum := []uint32{0, 0}
	checkCount := 24
	err := VfsChecksum(headerBuffer, uint32(checkCount), checksum, checksum)
	assert.NoError(t, err)
	sum := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(sum, checksum[0])
	for i := 0; i < 4; i++ {
		assert.Equal(t, headerBuffer[i+24], sum[i])
	}
	binary.BigEndian.PutUint32(sum, checksum[1])
	for i := 0; i < 4; i++ {
		assert.Equal(t, headerBuffer[i+28], sum[i])
	}
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
