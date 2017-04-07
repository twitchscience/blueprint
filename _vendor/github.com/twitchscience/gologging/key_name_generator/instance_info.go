package key_name_generator

import "os"

type InstanceInfoFetcher interface {
	GetHost() string
	GetClusterName() string
	GetAutoScaleGroup() string
}

type EnvInstanceFetcher struct{}

// We provide service name incase there are multiple types of services
// running on this box
func BuildInstanceInfo(fetcher InstanceInfoFetcher, serviceName string, loggingDir string) *InstanceInfo {
	host := fetcher.GetHost()
	cluster := fetcher.GetClusterName()
	asg := fetcher.GetAutoScaleGroup()
	return &InstanceInfo{
		Service:        serviceName,
		Cluster:        cluster,
		AutoScaleGroup: asg,
		Node:           host,
		LoggingDir:     loggingDir,
	}
}

func (e *EnvInstanceFetcher) GetHost() string {
	host := os.Getenv("HOST")
	if host == "" {
		host = "UNKNOWN"
	}
	return host
}

func (e *EnvInstanceFetcher) GetClusterName() string {
	cluster := os.Getenv("CLOUD_CLUSTER")
	if cluster == "" {
		cluster = "UNKNOWN"
	}
	return cluster
}

func (e *EnvInstanceFetcher) GetAutoScaleGroup() string {
	asg := os.Getenv("CLOUD_AUTO_SCALE_GROUP")
	if asg == "" {
		asg = "UNKNOWN"
	}
	return asg
}
