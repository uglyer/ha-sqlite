package walfs

import (
	"github.com/stretchr/testify/assert"
	"math/rand"
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
	wal := NewWalFS()
	pageSize := 4096
	buffer := make([]byte, pageSize)
	for i := 0; i < pageSize; i++ {
		buffer[i] = uint8(rand.Uint32())
	}

	t.Run("without wal file test", func(t *testing.T) {
		err := wal.VfsApplyLog("test", buffer)
		assert.NoError(t, err)
	})

}
