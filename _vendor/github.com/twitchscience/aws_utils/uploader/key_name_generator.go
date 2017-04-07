package uploader

type S3KeyNameGenerator interface {
	GetKeyName(string) string
}
