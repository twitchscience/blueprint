#!/bin/bash --
set -e -u -o pipefail

PROJECT_NAME=$1
if [ -z $PROJECT_NAME ]; then
    echo "Error: expected Project Name as first argument"
    exit 1
fi

export GOPATH="/home/vagrant/go"
export SRCDIR="${GOPATH}/src/github.com/twitchscience/${PROJECT_NAME}"
export PKGDIR="/tmp/pkg"
export DEPLOYDIR="${PKGDIR}/deploy"

mkdir -p ${DEPLOYDIR}/{bin,data}
cp -R ${SRCDIR}/${PROJECT_NAME} ${DEPLOYDIR}/bin
cp ${SRCDIR}/build/scripts/* ${DEPLOYDIR}/bin
cp -R ${SRCDIR}/build/config ${DEPLOYDIR}
cp -R ${SRCDIR}/static/* ${DEPLOYDIR}/data

echo "Setting up transforms"
sudo apt-get -y install libgeoip-dev libgeoip1 || true # silence whining about stdin
go get github.com/twitchscience/spade/transformer_dumper
${GOPATH}/bin/transformer_dumper -outFile "${DEPLOYDIR}/config/transforms_available.json"
