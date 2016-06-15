#!/bin/bash --
set -e -u -o pipefail

SCIENCE_DIR="/opt/science"
CONFIG_DIR="${SCIENCE_DIR}/blueprint/config"

cd -- "$(dirname -- "$0")"
eval "$(curl --silent 169.254.169.254/latest/user-data/)"
export HOST="$(curl --silent 169.254.169.254/latest/meta-data/hostname)"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
aws s3 cp --region us-west-2 "$CONFIG_PREFIX/conf.sh" conf.sh
aws s3 cp --region us-west-2 "$CONFIG_PREFIX/conf.json" $CONFIG_DIR/conf.json
source conf.sh

exec ./blueprint "$@"                                        \
  -scoopURL="${SCOOP_URL}"                                   \
  -bpdbConnection="${BLUEPRINT_DB_URL}"                      \
  -cookieSecret=${COOKIE_SECRET}                             \
  -clientID=${CLIENT_ID}                                     \
  -clientSecret=${CLIENT_SECRET}                             \
  -githubServer=${GITHUB_SERVER}                             \
  -transformConfig="${CONFIG_DIR}/transforms_available.json" \
  -requiredOrg=${REQUIRED_ORG}                               \
  -staticfiles="${SCIENCE_DIR}/nginx/html"                   \
  -ingesterURL="${INGESTER_URL}"                             \
  -config="${CONFIG_DIR}/conf.json"

