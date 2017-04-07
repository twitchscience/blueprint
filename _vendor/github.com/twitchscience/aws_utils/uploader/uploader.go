package uploader

import (
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/twitchscience/aws_utils/common"
)

type FileTypeHeader string

const (
	Gzip FileTypeHeader = "application/x-gzip"
	Text FileTypeHeader = "text/plain"
)

// Factory is an interface to an object that makes new Uploader instances
type Factory interface {
	NewUploader() Uploader
}

// Uploader is an interface for uploading files
type Uploader interface {
	Upload(*UploadRequest) (*UploadReceipt, error)
}

type factory struct {
	bucket           string
	keynameGenerator S3KeyNameGenerator
	s3Uploader       s3manageriface.UploaderAPI
}

type uploader struct {
	bucket           string
	keynameGenerator S3KeyNameGenerator
	s3Uploader       s3manageriface.UploaderAPI
}

func NewFactory(bucket string, keynameGenerator S3KeyNameGenerator, s3Uploader s3manageriface.UploaderAPI) Factory {
	return &factory{
		bucket:           bucket,
		keynameGenerator: keynameGenerator,
		s3Uploader:       s3Uploader,
	}
}

func (f *factory) NewUploader() Uploader {
	return &uploader{
		bucket:           f.bucket,
		keynameGenerator: f.keynameGenerator,
		s3Uploader:       f.s3Uploader,
	}
}

var retrier = &common.Retrier{
	Times:         3,
	BackoffFactor: 2,
}

func (worker *uploader) Upload(req *UploadRequest) (*UploadReceipt, error) {
	file, err := os.Open(req.Filename)
	if err != nil {
		return nil, err
	}
	// This means that if we fail to talk to s3 we still remove the file.
	// I think that this is the correct behavior as we dont want to cause
	// a HD overflow in case of a http timeout.
	defer os.Remove(req.Filename)
	keyName := worker.keynameGenerator.GetKeyName(req.Filename)

	err = retrier.Retry(func() error {
		// We need to seek to ensure that the retries read from the start of the file
		file.Seek(0, 0)

		_, e := worker.s3Uploader.Upload(&s3manager.UploadInput{
			Bucket:      aws.String(worker.bucket),
			Key:         aws.String(keyName),
			ACL:         aws.String("bucket-owner-full-control"),
			ContentType: aws.String(string(req.FileType)),
			Body:        file,
		})

		return e
	})
	if err != nil {
		return nil, err
	}
	return &UploadReceipt{
		Path:    req.Filename,
		KeyName: worker.bucket + "/" + keyName,
	}, nil
}
