package bpdb

import "testing"

func TestUpload(t *testing.T) {
	uploader := NewS3Uploader()

	data := []byte("weeweeeeeeeeeeeeee")
	uploader.Upload(data, EVENT_METADATA_BUCKET)
}
