package memfs

import (
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"testing"
)

func TestMemFS(t *testing.T) {
	rootFS := NewFS()

	testFile, err := rootFS.OpenFile("test.txt", os.O_CREATE, 0600)
	assert.NoError(t, err)

	body := []byte("memFS")
	writeCount, err := testFile.WriteAt(body, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(body), writeCount)

	info, err := testFile.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(body)), info.Size())

	// 完整读取测试
	readBody5 := make([]byte, len(body)+2)
	readCount, err := testFile.ReadAt(readBody5, 0)
	assert.NoError(t, err)
	assert.Equal(t, len(body), readCount)
	assert.Equal(t, body, readBody5[:readCount])

	// 分页测试
	pageSize := 8192
	pageBody := make([]byte, pageSize)
	for i := 0; i < pageSize; i++ {
		pageBody[i] = byte(math.Round(1.0) * 255)
	}
	writeCount, err = testFile.WriteAt(pageBody, int64(len(body)))
	assert.NoError(t, err)
	assert.Equal(t, len(pageBody), writeCount)

	info, err = testFile.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(len(body)+len(pageBody)), info.Size())

	// 分片读取测试
	readCount, err = testFile.ReadAt(readBody5, int64(len(body)-1))
	assert.NoError(t, err)
	assert.Equal(t, len(readBody5), readCount)
	assert.Equal(t, body[len(body)-1], readBody5[0])
	for i := 1; i < readCount; i++ {
		assert.Equal(t, pageBody[i-1], readBody5[i])
	}
}
