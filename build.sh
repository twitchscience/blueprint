#!/bin/bash --
set -euo pipefail

# ossareh(20150109): Perhaps use something like:
# http://stackoverflow.com/questions/192249/how-do-i-parse-command-line-arguments-in-bash
PROJECT=$1
BRANCH=$2
SOURCE_AMI=$3
VPC=$4
SUBNET=$5
SECURITY_GROUP=$6

# I hate boolean args, but I'm not sure how to handle this.
USE_PRIVATE_IP=${7:-"false"}

sudo apt-get install -y pkg-config
go get github.com/twitchscience/spade/transformer_dumper
transformer_dumper -outFile build/config/transforms_available.json

export GOARCH=amd64
export GOOS=linux
export GOBIN="/tmp/${PROJECT}_build_$$"

go vet ./... # Replace with gometalinter soon
go test -v ./...
go install -v ./...

packer                                         \
    -machine-readable build                    \
    -var "project=${PROJECT}"                  \
    -var "branch=${BRANCH}"                    \
    -var "binary_dir=${GOBIN}"                 \
    -var "source_ami=${SOURCE_AMI}"            \
    -var "vpc_id=${VPC}"                       \
    -var "subnet_id=${SUBNET}"                 \
    -var "security_group_id=${SECURITY_GROUP}" \
    -var "use_private_ip=${USE_PRIVATE_IP}"    \
    build/packer.json
