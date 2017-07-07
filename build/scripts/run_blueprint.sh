#!/bin/bash --
set -e -u -o pipefail

SCIENCE_DIR="/opt/science"
CONFIG_DIR="${SCIENCE_DIR}/blueprint/config"

source /etc/environment

cd -- "$(dirname -- "$0")"
export HOST="$(curl --silent 169.254.169.254/latest/meta-data/hostname)"
export CONFIG_PREFIX="s3://$S3_CONFIG_BUCKET/$VPC_SUBNET_TAG/$CLOUD_APP/$CLOUD_ENVIRONMENT"
export AWS_REGION=us-west-2
aws s3 cp --region "$AWS_REGION" "$CONFIG_PREFIX/conf.sh" conf.sh
aws s3 cp --region "$AWS_REGION" "$CONFIG_PREFIX/conf.json" "$CONFIG_DIR/conf.json"
source conf.sh

exec ./blueprint "$@"                                        \
  -enableAuth=${ENABLE_AUTH}                                 \
  -bpdbConnection="${BLUEPRINT_DB_URL}"                      \
  -cookieSecret=${COOKIE_SECRET}                             \
  -clientID=${CLIENT_ID}                                     \
  -clientSecret=${CLIENT_SECRET}                             \
  -githubServer=${GITHUB_SERVER}                             \
  -requiredOrg=${REQUIRED_ORG}                               \
  -adminTeam=${ADMIN_TEAM}                                   \
  -staticfiles="${SCIENCE_DIR}/nginx/html"                   \
  -ingesterURL="${INGESTER_URL}"                             \
  -rollbarToken="${ROLLBAR_TOKEN}"                           \
  -rollbarEnvironment="${CLOUD_ENVIRONMENT}"                 \
  -slackbotURL="${SLACKBOT_URL}"                             \
  -config="${CONFIG_DIR}/conf.json"
