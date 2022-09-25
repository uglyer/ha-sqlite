package memfs

import (
	"github.com/stretchr/testify/assert"
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

}
