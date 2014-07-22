#!/bin/bash --
set -e -o pipefail -u

PROJECT_NAME=$1
if [ -z $PROJECT_NAME ]; then
    echo "Error: expected Project Name as first argument"
    exit 1
fi

export GOPATH="/home/vagrant/go"
export SRCDIR="${GOPATH}/src/github.com/twitchscience/${PROJECT_NAME}"
export PATH=${PATH}:${GOPATH}/bin

cd ${SRCDIR}
godep go test -v ./...
