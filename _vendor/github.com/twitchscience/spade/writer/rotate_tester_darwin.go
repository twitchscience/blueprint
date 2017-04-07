package writer

import (
	"os"
	"syscall"
	"time"
)

func isRotateNeeded(inode os.FileInfo, name string, conditions RotateConditions) (bool, time.Time) {
	stats := inode.Sys().(*syscall.Stat_t)
	createdAt := time.Unix(stats.Atimespec.Sec, stats.Atimespec.Nsec)
	return inode.Size() > conditions.MaxLogSize || (time.Since(createdAt)) > conditions.MaxTimeAllowed, createdAt
}
