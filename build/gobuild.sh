#!/bin/bash --
set -e -u -o pipefail

PROJECT_NAME=$1
if [ -z $PROJECT_NAME ]; then
    echo "Error: expected Project Name as first argument"
    exit 1
fi

# Setup Go
GOROOT="/usr/local/go"
sudo mkdir -p ${GOROOT}
curl -sL http://golang.org/dl/go1.3.linux-amd64.tar.gz | sudo tar -C ${GOROOT} -xz --strip-components=1
sudo ln -s ${GOROOT}/bin/* /usr/local/bin

# Build Code
BASEDIR="/home/vagrant"
export GOPATH="${BASEDIR}/go"
mkdir -p ${GOPATH}/{bin,pkg,src}
GOSRCDIR="${GOPATH}/src/github.com/twitchscience/${PROJECT_NAME}"
SRCDIR="${BASEDIR}/src"
PATH=${PATH}:${GOPATH}/bin

# perform the build
mkdir -p ${GOSRCDIR}
cp -R ${SRCDIR}/* ${GOSRCDIR}
cd ${GOSRCDIR}

go get -t github.com/tools/godep
godep go clean
godep go build -v

