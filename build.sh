#!/bin/bash --
set -euo pipefail

PROJECT=$1
BRANCH=$2
SOURCE_AMI=ami-46f70b3e
VPC=$4
SUBNET=$5
SECURITY_GROUP=$6

USE_PRIVATE_IP=${7:-"false"}

export GOARCH=amd64
export GOOS=linux
export GOBIN="/tmp/${PROJECT}_build_$$"

bash run_tests.sh
go install -v ./...

gometalinter ./... --disable=gocyclo  --disable=dupl --disable=gas --deadline 90s

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
    build/packer.json | tee build.log

AMIREF=`grep 'amazon-ebs,artifact,0,id,' build.log`
echo ${AMIREF##*:} > amireference
