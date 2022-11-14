package s3

import (
	"fmt"
	"github.com/minio/minio-go/v6"
	"github.com/uglyer/ha-sqlite/log"
	"path"
)

type S3Client struct {
	client *minio.Client
	config *S3Config
}

// NewS3Client 构建新的 S3 客户端
func NewS3Client(config *S3Config) (*S3Client, error) {
	client, err := minio.New(config.Endpoint, config.AccessKey, config.SecretKey, !config.DisableSSL)
	if err != nil {
		return nil, err
	}
	exists, err := client.BucketExists(config.Bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, fmt.Errorf("s3 bycket(%s) is null", config.Bucket)
	}
	return &S3Client{
		client: client,
		config: config,
	}, nil
}

//Snapshot 快照操作, remotePath 不包含 PrefixPath
func (c *S3Client) Snapshot(dbPath string, remotePath string) error {
	objectName := path.Join(c.config.PrefixPath, remotePath)
	log.Info(fmt.Sprintf("S3[minio] Snapshot:%s->%s", dbPath, objectName))
	_, err := c.client.FPutObject(c.config.Bucket, objectName, dbPath, minio.PutObjectOptions{ContentType: "application/octet-stream"})
	return err
}

//Restore 恢复操作, remotePath 不包含 PrefixPath
func (c *S3Client) Restore(dbPath string, remotePath string) error {
	objectName := path.Join(c.config.PrefixPath, remotePath)
	log.Info(fmt.Sprintf("S3[minio] Restore:%s<-%s", dbPath, objectName))
	return c.client.FGetObject(c.config.Bucket, objectName, dbPath, minio.GetObjectOptions{})
}
