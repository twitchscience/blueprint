#!/bin/bash --
set -e -u -o pipefail

CONFIG_DIR="/opt/science/blueprint/config"
SCIENCE_DIR="/opt/science"

cd -- "$(dirname -- "$0")"
eval "$(curl --silent 169.254.169.254/latest/user-data/)"
export HOST="$(curl --silent 169.254.169.254/latest/meta-data/hostname)"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
export AWS_REGION=us-west-2
export AWS_DEFAULT_REGION=$AWS_REGION # aws-cli uses AWS_DEFAULT_REGION, aws-sdk-go uses AWS_REGION
aws s3 cp "$CONFIG_PREFIX/conf.sh" conf.sh
source conf.sh

export GOMAXPROCS="2"

exec ./schema_suggestor \
  -url="${SCOOP_URL}" \
  -transformConfig="${CONFIG_DIR}/transforms_available.json" \
  -staticfiles="${SCIENCE_DIR}/nginx/html/events"
