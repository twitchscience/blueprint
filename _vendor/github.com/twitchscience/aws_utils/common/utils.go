package common

import (
	"strings"
	"time"
)

type Retrier struct {
	Times         int
	BackoffFactor int
}

func (r *Retrier) Retry(fn func() error) error {
	var err error
	for i := 1; i <= r.Times; i++ {
		err = fn()
		if err == nil {
			return nil
		}
		time.Sleep(time.Duration(i*r.BackoffFactor) * time.Second)
	}
	return err
}

// Convert a URL with or without s3:// on the front to its s3:// form
func NormalizeS3URL(rawurl string) string {
	if strings.HasPrefix(rawurl, "s3://") {
		return rawurl
	}
	return "s3://" + rawurl
}
