#!/bin/bash --
set -e -u -o pipefail

SCIENCE_DIR="/opt/science"
CONFIG_DIR="${SCIENCE_DIR}/blueprint/config"

cd -- "$(dirname -- "$0")"
eval "$(curl --silent 169.254.169.254/latest/user-data/)"
export HOST="$(curl --silent 169.254.169.254/latest/meta-data/hostname)"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
aws s3 cp --region us-west-2 "$CONFIG_PREFIX/conf.sh" conf.sh
source conf.sh

exec ./blueprint \
  -scoopURL="${SCOOP_URL}" \
  -transformConfig="${CONFIG_DIR}/transforms_available.json" \
  -staticfiles="${SCIENCE_DIR}/nginx/html"
