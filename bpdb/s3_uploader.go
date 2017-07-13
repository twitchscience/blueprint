package bpdb

import (
	"bytes"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/twitchscience/aws_utils/logger"
)

// Reconsider this file name/location when this code works

// Buckets
const (
	EventMetadataBucket = "event-metadata-test"
)

// S3Uploader helps upload responses to S3
type S3Uploader struct {
	// Uploader uploads files from S3
	// Uploader s3manageriface.DownloaderAPI
	Uploader *s3manager.Uploader
}

// NewS3Uploader returns a new S3Uploader
func NewS3Uploader() *S3Uploader {
	s := session.Must(session.NewSession())

	uploader := s3manager.NewUploader(s)

	return &S3Uploader{Uploader: uploader}
}

// Upload uploads data to S3\
func (s3uploader *S3Uploader) Upload(data []byte, bucket string) error {
	key := "testing-testing-fred"
	upParams := &s3manager.UploadInput{
		Bucket: &bucket,
		Key:    &key,
		Body:   bytes.NewReader(data),
	}

	result, err := s3uploader.Uploader.Upload(upParams)

	logger.Info(err)
	logger.Info(result)

	return nil
}
