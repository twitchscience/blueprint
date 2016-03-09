package environment

import (
	"os"
)

func cloudEnv() string {
	ce := os.Getenv("CLOUD_DEV_PHASE")
	if ce == "" {
		ce = os.Getenv("CLOUD_ENVIRONMENT")
	}
	return ce
}

// Returns a name for the environment that we are running in.
// Preference is for CLOUD_DEV_PHASE, then CLOUD_ENVIRONMENT,
// and finally for $USER-dev.
func GetCloudEnv() string {
	ce := cloudEnv()
	if ce == "" {
		ce = os.Getenv("USER") + "-dev"
	}
	return ce
}

func IsProd() bool {
	ce := cloudEnv()
	if ce == "" {
		return false
	}
	return true
}
