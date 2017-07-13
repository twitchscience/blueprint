package bpdb

import (
	"testing"

	"github.com/twitchscience/aws_utils/logger"
)

func TestUpload(t *testing.T) {
	uploader := NewS3Uploader()

	data := []byte("weeweeeeeeeeeeeeee")
	err := uploader.Upload(data, EventMetadataBucket)

	if err != nil {
		logger.Info("Error")
	}
}
