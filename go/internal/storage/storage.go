package storage

import (
	"context"
	"io"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type Storage struct {
	client        *minio.Client
	UploadsBucket string
}

func New(endpointHost, minioAccess, minioSecret, uploadsBucket string, secure bool) (*Storage, error) {
	minioClient, err := minio.New(endpointHost, &minio.Options{
		Creds:  credentials.NewStaticV4(minioAccess, minioSecret, ""),
		Secure: secure,
	})
	if err != nil {
		return nil, err
	}
	return &Storage{
		client:        minioClient,
		UploadsBucket: uploadsBucket,
	}, nil
}

func (s *Storage) PutObject(ctx context.Context, bucket, objectName string, reader io.Reader, objectSize int64, opts minio.PutObjectOptions) (minio.UploadInfo, error) {
	return s.client.PutObject(ctx, bucket, objectName, reader, objectSize, opts)
}

func (s *Storage) StatObject(ctx context.Context, bucket, objectName string, opts minio.StatObjectOptions) (minio.ObjectInfo, error) {
	return s.client.StatObject(ctx, bucket, objectName, opts)
}

func (s *Storage) GetObject(ctx context.Context, bucket, objectName string, opts minio.GetObjectOptions) (*minio.Object, error) {
	return s.client.GetObject(ctx, bucket, objectName, opts)
}
