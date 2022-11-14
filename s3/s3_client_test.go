package s3_test

import (
	"database/sql"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	_ "github.com/uglyer/go-sqlite3" // Go SQLite bindings with wal hook
	"github.com/uglyer/ha-sqlite/s3"
	"io/ioutil"
	"os"
	"path"
	"testing"
)

func newClient(t *testing.T) *s3.S3Client {
	config := &s3.S3Config{
		Enabled:    true,
		AccessKey:  os.Getenv("AccessKey"),
		SecretKey:  os.Getenv("SecretKey"),
		Endpoint:   os.Getenv("Endpoint"),
		Region:     os.Getenv("Region"),
		DisableSSL: true,
		Bucket:     os.Getenv("Bucket"),
		PrefixPath: "s3-store-dev",
	}
	assert.NotEmpty(t, config.AccessKey)
	assert.NotEmpty(t, config.SecretKey)
	assert.NotEmpty(t, config.Endpoint)
	assert.NotEmpty(t, config.Bucket)
	client, err := s3.NewS3Client(config)
	assert.NoError(t, err)
	return client
}

func TestS3Client_SnapshotAndRestore(t *testing.T) {
	fileRaw, err := ioutil.TempFile("", "ha-sqlite-s3-test")
	fileRestore, err := ioutil.TempFile("", "ha-sqlite-s3-test")
	defer fileRaw.Close()
	// 仅为了生成一个随机名称
	err = fileRestore.Close()
	assert.NoError(t, err)
	err = os.Remove(fileRestore.Name())
	assert.NoError(t, err)
	id, err := uuid.NewRandom()
	assert.NoError(t, err)
	fileRemote := path.Join("test", fmt.Sprintf("%s.db", id.String()))
	assert.NoError(t, err)
	db, err := sql.Open("sqlite3", fileRaw.Name())
	assert.NoError(t, err)
	defer db.Close()
	_, err = db.Exec("CREATE TABLE foo (id integer not null primary key, name text)")
	assert.NoError(t, err)
	err = db.Close()
	assert.NoError(t, err)
	client := newClient(t)
	err = client.Snapshot(fileRaw.Name(), fileRemote)
	assert.NoError(t, err)
	info, err := client.StatObject(fileRemote)
	assert.NoError(t, err)
	rawStat, err := fileRaw.Stat()
	assert.NoError(t, err)
	assert.Equal(t, rawStat.Size(), info.Size)
	err = client.Restore(fileRestore.Name(), fileRemote)
	assert.NoError(t, err)
	rawBytes, err := ioutil.ReadFile(fileRaw.Name())
	assert.NoError(t, err)
	restoreBytes, err := ioutil.ReadFile(fileRestore.Name())
	assert.NoError(t, err)
	assert.Equal(t, len(rawBytes), len(restoreBytes))
	for i := 0; i < len(rawBytes); i++ {
		assert.Equal(t, rawBytes[i], restoreBytes[i])
	}
}
