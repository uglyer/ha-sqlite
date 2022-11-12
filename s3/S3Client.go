package s3

import (
	"fmt"
	"github.com/minio/minio-go/v6"
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
